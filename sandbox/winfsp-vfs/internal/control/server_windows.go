//go:build windows

package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	portproxyv1 "github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/portproxypb"
	"github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/runtimeconfig"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	vfsServiceName    = "ChevalierVFS"
	maxFileRPCBytes   = 128 << 20
	maxDirectoryItems = 100_000
)

type Runtime struct {
	configPath  string
	stopping    atomic.Bool
	shutdown    sync.Once
	shutdownErr error
}

type outputFrame struct {
	stderr bool
	data   []byte
}

func NewRuntime(configPath string) *Runtime {
	return &Runtime{configPath: configPath}
}

func (r *Runtime) Run(ctx context.Context) error {
	writeGuestStatus("waiting-for-runtime-config", nil)
	config, err := waitForConfig(ctx, r.configPath)
	if err != nil {
		writeGuestStatus("failed", err)
		return err
	}
	writeRuntimeStatus(runtimeconfig.DefaultFirstBootStatus, "waiting-for-desktop", nil)
	if err := runtimeconfig.FinalizeFirstBoot(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			writeRuntimeStatus(runtimeconfig.DefaultFirstBootStatus, "failed", err)
		}
		return err
	}
	writeRuntimeStatus(runtimeconfig.DefaultFirstBootStatus, "complete", nil)
	writeGuestStatus("initializing-state-volume", nil)
	if err := initializeStateVolume(ctx); err != nil {
		writeGuestStatus("failed", err)
		return err
	}
	writeGuestStatus("starting-vfs", nil)
	if err := startService(ctx, vfsServiceName, svc.Running, 90*time.Second); err != nil {
		err = fmt.Errorf("start VFS service: %w", err)
		writeGuestStatus("failed", err)
		return err
	}
	if err := ensureControlFirewall(ctx, config.Control.ListenAddress); err != nil {
		writeGuestStatus("failed", err)
		return err
	}
	tokenBytes, err := os.ReadFile(config.Control.TokenFile)
	if err != nil {
		writeGuestStatus("failed", err)
		return fmt.Errorf("read control token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		err = fmt.Errorf("control token is empty")
		writeGuestStatus("failed", err)
		return err
	}
	listener, err := net.Listen("tcp", config.Control.ListenAddress)
	if err != nil {
		writeGuestStatus("failed", err)
		return fmt.Errorf("listen for guest control: %w", err)
	}
	defer listener.Close()
	server := grpc.NewServer(
		grpc.UnaryInterceptor(unaryAuth(token)),
		grpc.StreamInterceptor(streamAuth(token)),
		grpc.MaxRecvMsgSize(maxFileRPCBytes+1<<20),
		grpc.MaxSendMsgSize(maxFileRPCBytes+1<<20),
	)
	service := &guestService{runtime: r}
	portproxyv1.RegisterShellExecServer(server, service)
	portproxyv1.RegisterPortProxyServer(server, service)

	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	writeGuestStatus("ready", nil)
	select {
	case <-ctx.Done():
		if err := r.stop(context.Background()); err != nil {
			server.Stop()
			return err
		}
		server.GracefulStop()
		return nil
	case err := <-serveResult:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("serve guest control: %w", err)
	}
}

func writeGuestStatus(phase string, runtimeError error) {
	writeRuntimeStatus(runtimeconfig.DefaultGuestStatus, phase, runtimeError)
}

func writeRuntimeStatus(path string, phase string, runtimeError error) {
	status := struct {
		SchemaVersion int    `json:"schemaVersion"`
		Phase         string `json:"phase"`
		Error         string `json:"error,omitempty"`
		UpdatedAt     string `json:"updatedAt"`
	}{
		SchemaVersion: 1,
		Phase:         phase,
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	if runtimeError != nil {
		status.Error = runtimeError.Error()
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	_ = runtimeconfig.WriteFileAtomic(path, encoded)
}

func ensureControlFirewall(ctx context.Context, listenAddress string) error {
	_, port, err := net.SplitHostPort(listenAddress)
	if err != nil {
		return fmt.Errorf("parse control listen address: %w", err)
	}
	const ruleName = "Chevalier guest control"
	_ = exec.CommandContext(ctx, "netsh.exe", "advfirewall", "firewall", "delete", "rule", "name="+ruleName).Run()
	output, err := exec.CommandContext(
		ctx,
		"netsh.exe",
		"advfirewall",
		"firewall",
		"add",
		"rule",
		"name="+ruleName,
		"dir=in",
		"action=allow",
		"protocol=TCP",
		"localport="+port,
		"remoteip=10.0.2.2",
		"profile=any",
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("configure guest control firewall: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (r *Runtime) stop(ctx context.Context) error {
	r.shutdown.Do(func() {
		r.stopping.Store(true)
		r.shutdownErr = stopService(ctx, vfsServiceName, 90*time.Second)
	})
	return r.shutdownErr
}

func waitForConfig(ctx context.Context, configuredPath string) (runtimeconfig.Config, error) {
	seedDeadline := time.Now().Add(30 * time.Second)
	for {
		if config, imported, err := runtimeconfig.ImportSeed(); err != nil {
			return runtimeconfig.Config{}, err
		} else if imported {
			return config, nil
		}
		if time.Now().After(seedDeadline) {
			if config, err := runtimeconfig.Load(configuredPath); err == nil {
				return config, nil
			} else if !os.IsNotExist(err) {
				return runtimeconfig.Config{}, err
			}
		}
		select {
		case <-ctx.Done():
			return runtimeconfig.Config{}, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func initializeStateVolume(ctx context.Context) error {
	const script = `C:\Program Files\Chevalier\initialize-state.ps1`
	command := exec.CommandContext(ctx, `C:\Program Files\PowerShell\7\pwsh.exe`, "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", script)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("initialize VFS state volume: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

type guestService struct {
	portproxyv1.UnimplementedShellExecServer
	portproxyv1.UnimplementedPortProxyServer
	runtime *Runtime
}

func (s *guestService) Exec(stream portproxyv1.ShellExec_ExecServer) error {
	if s.runtime.stopping.Load() {
		return status.Error(codes.Unavailable, "guest is preparing to stop")
	}
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "read exec start: %v", err)
	}
	start := first.GetStart()
	if start == nil || len(start.Args) == 0 || strings.TrimSpace(start.Args[0]) == "" {
		return status.Error(codes.InvalidArgument, "first exec message must contain argv")
	}
	if start.Detach {
		return status.Error(codes.Unimplemented, "detached commands are not implemented on Windows")
	}
	if len(start.Args) > 4096 {
		return status.Error(codes.InvalidArgument, "exec argv is too large")
	}

	commandContext, commandCancel := context.WithCancel(stream.Context())
	defer commandCancel()
	if start.Timeout != nil && *start.Timeout > 0 {
		var timeoutCancel context.CancelFunc
		commandContext, timeoutCancel = context.WithTimeout(commandContext, time.Duration(*start.Timeout)*time.Second)
		defer timeoutCancel()
	}
	command := exec.CommandContext(commandContext, start.Args[0], start.Args[1:]...)
	command.Env = mergeEnvironment(os.Environ(), start.Env)
	command.Stdin = nil
	stdin, err := command.StdinPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "open command stdin: %v", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "open command stdout: %v", err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "open command stderr: %v", err)
	}
	command.SysProcAttr = windowsProcessAttributes()
	if err := command.Start(); err != nil {
		return status.Errorf(codes.Internal, "start command: %v", err)
	}
	job, err := assignKillOnCloseJob(command.Process.Pid)
	if err != nil {
		_ = command.Process.Kill()
		return status.Errorf(codes.Internal, "contain command process tree: %v", err)
	}
	defer windows.CloseHandle(job)

	go func() {
		defer stdin.Close()
		for {
			request, err := stream.Recv()
			if err != nil {
				return
			}
			if data := request.GetStdinData(); len(data) > 0 {
				if _, err := stdin.Write(data); err != nil {
					return
				}
			}
		}
	}()

	frames := make(chan outputFrame, 32)
	var readers sync.WaitGroup
	readers.Add(2)
	go readPipe(commandContext, stdout, false, frames, &readers)
	go readPipe(commandContext, stderr, true, frames, &readers)
	go func() {
		readers.Wait()
		close(frames)
	}()
	waitResult := make(chan error, 1)
	go func() { waitResult <- command.Wait() }()

	exitCode := 0
	for waitResult != nil || frames != nil {
		select {
		case frame, ok := <-frames:
			if !ok {
				frames = nil
				continue
			}
			response := &portproxyv1.ExecResponse{}
			if frame.stderr {
				response.Response = &portproxyv1.ExecResponse_StderrData{StderrData: frame.data}
			} else {
				response.Response = &portproxyv1.ExecResponse_StdoutData{StdoutData: frame.data}
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		case waitErr := <-waitResult:
			if waitErr != nil {
				if command.ProcessState != nil {
					exitCode = command.ProcessState.ExitCode()
				} else {
					exitCode = 1
				}
				if errors.Is(commandContext.Err(), context.DeadlineExceeded) {
					exitCode = 124
				}
			}
			waitResult = nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	return stream.Send(&portproxyv1.ExecResponse{Response: &portproxyv1.ExecResponse_ExitCode{ExitCode: int32(exitCode)}})
}

func readPipe(ctx context.Context, reader io.Reader, stderr bool, frames chan<- outputFrame, readers *sync.WaitGroup) {
	defer readers.Done()
	buffer := make([]byte, 32*1024)
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			data := append([]byte(nil), buffer[:count]...)
			select {
			case frames <- outputFrame{stderr: stderr, data: data}:
			case <-ctx.Done():
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func mergeEnvironment(base []string, overrides map[string]string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	for _, entry := range base {
		if key, value, ok := strings.Cut(entry, "="); ok {
			values[strings.ToUpper(key)] = key + "=" + value
		}
	}
	for key, value := range overrides {
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, '\x00') {
			continue
		}
		values[strings.ToUpper(key)] = key + "=" + value
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func windowsProcessAttributes() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
}

func assignKillOnCloseJob(pid int) (windows.Handle, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, err
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)),
		uint32(unsafe.Sizeof(limits)),
	); err != nil {
		windows.CloseHandle(job)
		return 0, err
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		windows.CloseHandle(job)
		return 0, err
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		windows.CloseHandle(job)
		return 0, err
	}
	return job, nil
}

func (s *guestService) PrepareShutdown(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if err := s.runtime.stop(ctx); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "drain VFS service: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *guestService) ReadFile(_ context.Context, request *portproxyv1.ReadFileRequest) (*portproxyv1.ReadFileResponse, error) {
	data, err := os.ReadFile(request.Path)
	if err != nil {
		return nil, mapFileError(err)
	}
	if len(data) > maxFileRPCBytes {
		return nil, status.Error(codes.ResourceExhausted, "file exceeds RPC size limit")
	}
	return &portproxyv1.ReadFileResponse{Data: data}, nil
}

func (s *guestService) WriteFile(_ context.Context, request *portproxyv1.WriteFileRequest) (*emptypb.Empty, error) {
	if len(request.Data) > maxFileRPCBytes {
		return nil, status.Error(codes.ResourceExhausted, "file exceeds RPC size limit")
	}
	if request.CreateParents {
		if err := os.MkdirAll(filepath.Dir(request.Path), 0o755); err != nil {
			return nil, mapFileError(err)
		}
	}
	if err := runtimeconfig.WriteFileAtomic(request.Path, request.Data); err != nil {
		return nil, mapFileError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *guestService) ListDirectory(_ context.Context, request *portproxyv1.ListDirectoryRequest) (*portproxyv1.ListDirectoryResponse, error) {
	entries, err := os.ReadDir(request.Path)
	if err != nil {
		return nil, mapFileError(err)
	}
	if len(entries) > maxDirectoryItems {
		return nil, status.Error(codes.ResourceExhausted, "directory exceeds RPC entry limit")
	}
	result := &portproxyv1.ListDirectoryResponse{Entries: make([]*portproxyv1.DirectoryEntry, 0, len(entries))}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, mapFileError(err)
		}
		result.Entries = append(result.Entries, &portproxyv1.DirectoryEntry{Name: entry.Name(), IsDir: entry.IsDir(), IsSymlink: info.Mode()&os.ModeSymlink != 0})
	}
	return result, nil
}

func (s *guestService) DeletePath(_ context.Context, request *portproxyv1.DeletePathRequest) (*emptypb.Empty, error) {
	if err := os.RemoveAll(request.Path); err != nil {
		return nil, mapFileError(err)
	}
	return &emptypb.Empty{}, nil
}

func mapFileError(err error) error {
	if os.IsNotExist(err) {
		return status.Error(codes.NotFound, err.Error())
	}
	if os.IsPermission(err) {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func startService(ctx context.Context, name string, target svc.State, timeout time.Duration) error {
	manager, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	service, err := manager.OpenService(name)
	if err != nil {
		return err
	}
	defer service.Close()
	state, err := service.Query()
	if err != nil {
		return err
	}
	if state.State != target {
		if err := service.Start(); err != nil && !errors.Is(err, windows.ERROR_SERVICE_ALREADY_RUNNING) {
			return err
		}
	}
	return waitServiceState(ctx, service, target, timeout)
}

func stopService(ctx context.Context, name string, timeout time.Duration) error {
	manager, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	service, err := manager.OpenService(name)
	if err != nil {
		return err
	}
	defer service.Close()
	state, err := service.Query()
	if err != nil {
		return err
	}
	if state.State == svc.Stopped {
		return nil
	}
	if _, err := service.Control(svc.Stop); err != nil && !errors.Is(err, windows.ERROR_SERVICE_NOT_ACTIVE) {
		return err
	}
	return waitServiceState(ctx, service, svc.Stopped, timeout)
}

func waitServiceState(ctx context.Context, service *mgr.Service, target svc.State, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		state, err := service.Query()
		if err != nil {
			return err
		}
		if state.State == target {
			if target == svc.Stopped && (state.Win32ExitCode != 0 || state.ServiceSpecificExitCode != 0) {
				return fmt.Errorf(
					"service stopped with Win32 exit code %d and service exit code %d",
					state.Win32ExitCode,
					state.ServiceSpecificExitCode,
				)
			}
			return nil
		}
		if target == svc.Running && state.State == svc.Stopped && (state.Win32ExitCode != 0 || state.ServiceSpecificExitCode != 0) {
			return fmt.Errorf(
				"service failed during startup with Win32 exit code %d and service exit code %d",
				state.Win32ExitCode,
				state.ServiceSpecificExitCode,
			)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("service did not reach state %d", target)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
