//go:build windows

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/gateway"
	guestmount "github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/mount"
	"github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/runtimeconfig"
	"github.com/winfsp/go-winfsp"
	"github.com/winfsp/go-winfsp/gofs"
	"golang.org/x/sys/windows/svc"
)

const serviceName = "ChevalierVFS"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "runtime configuration path")
	endpoint := flag.String("endpoint", "", "Chevalier VFS gateway owner endpoint")
	scope := flag.String("scope", "", "VFS scope below the gateway owner")
	tokenFile := flag.String("token-file", "", "file containing the gateway bearer token")
	stateDirectory := flag.String("state-directory", `C:\ProgramData\Chevalier\vfs-state\workspace`, "durable guest VFS state directory")
	mountpoint := flag.String("mount", `W:`, "WinFsp drive or directory mount point")
	statusFile := flag.String("status-file", `C:\ProgramData\Chevalier\vfs-state\workspace\status.json`, "periodically written publication status")
	drainTimeout := flag.Duration("drain-timeout", 30*time.Second, "publication drain timeout during orderly shutdown")
	debug := flag.Bool("debug", false, "write WinFsp request traces to stderr")
	debugLog := flag.String("debug-log", "", "write WinFsp request traces to this file instead of stderr")
	flag.Parse()
	isService, err := svc.IsWindowsService()
	if err != nil {
		return fmt.Errorf("detect Windows service context: %w", err)
	}
	if isService {
		path := *configPath
		if path == "" {
			path = runtimeconfig.DefaultConfigPath
		}
		return svc.Run(serviceName, &serviceHandler{configPath: path, debug: *debug, debugLog: *debugLog})
	}
	if *configPath != "" {
		options, err := optionsFromConfig(*configPath, *debug, *debugLog)
		if err != nil {
			return err
		}
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
		defer cancel()
		return runMount(ctx, options)
	}
	if *endpoint == "" || *scope == "" || *tokenFile == "" {
		return fmt.Errorf("--endpoint, --scope, and --token-file are required")
	}
	options := mountOptions{
		identity:       strings.Join([]string{"foreground", *endpoint, *scope}, "\n"),
		endpoint:       *endpoint,
		scope:          *scope,
		tokenFile:      *tokenFile,
		stateDirectory: *stateDirectory,
		mountpoint:     *mountpoint,
		statusFile:     *statusFile,
		drainTimeout:   *drainTimeout,
		debug:          *debug,
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	return runMount(ctx, options)
}

type mountOptions struct {
	identity       string
	endpoint       string
	scope          string
	tokenFile      string
	stateDirectory string
	mountpoint     string
	statusFile     string
	drainTimeout   time.Duration
	debug          bool
	debugLog       string
}

func optionsFromConfig(path string, debug bool, debugLog string) (mountOptions, error) {
	config, err := runtimeconfig.Load(path)
	if err != nil {
		return mountOptions{}, err
	}
	drainTimeout := 30 * time.Second
	if config.VFS.DrainTimeout != "" {
		drainTimeout, err = time.ParseDuration(config.VFS.DrainTimeout)
		if err != nil || drainTimeout <= 0 {
			return mountOptions{}, fmt.Errorf("invalid VFS drain timeout %q", config.VFS.DrainTimeout)
		}
	}
	return mountOptions{
		identity:       strings.Join([]string{config.VMID, config.VFS.Endpoint, config.VFS.Scope}, "\n"),
		endpoint:       config.VFS.Endpoint,
		scope:          config.VFS.Scope,
		tokenFile:      config.VFS.TokenFile,
		stateDirectory: config.VFS.StateDirectory,
		mountpoint:     config.VFS.Mountpoint,
		statusFile:     config.VFS.StatusFile,
		drainTimeout:   drainTimeout,
		debug:          debug,
		debugLog:       debugLog,
	}, nil
}

func runMount(ctx context.Context, options mountOptions) error {
	if err := guestmount.EnsureIdentity(options.stateDirectory, options.identity); err != nil {
		return err
	}
	token, err := os.ReadFile(options.tokenFile)
	if err != nil {
		return fmt.Errorf("read VFS token file: %w", err)
	}
	client, err := gateway.New(options.endpoint, strings.TrimSpace(string(token)), options.scope)
	if err != nil {
		return err
	}
	session, err := guestmount.Open(context.Background(), options.stateDirectory, client)
	if err != nil {
		return err
	}
	session.StartPublisher()
	defer session.Close()

	adapter, err := gofs.NewOptions(
		&fileSystemAdapter{session: session, debug: options.debug},
		gofs.WithCaseInsensitive(true),
		gofs.WithAttribReadOnlyTransMode(gofs.AttribReadOnlyBypass),
	)
	if err != nil {
		return fmt.Errorf("configure WinFsp adapter: %w", err)
	}
	mountOptions := []winfsp.Option{winfsp.FileSystemName("ChevalierVFS")}
	if options.debug {
		debugHandle := os.Stderr
		if options.debugLog != "" {
			debugHandle, err = os.OpenFile(options.debugLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				return fmt.Errorf("open WinFsp debug log: %w", err)
			}
			defer debugHandle.Close()
		}
		if err := winfsp.LoadWinFSP(); err != nil {
			return fmt.Errorf("load WinFsp for debug output: %w", err)
		}
		if err := winfsp.DebugLogSetHandle(syscall.Handle(debugHandle.Fd())); err != nil {
			return fmt.Errorf("configure WinFsp debug output: %w", err)
		}
		mountOptions = append(mountOptions, winfsp.Debug(true))
	}
	filesystem, err := winfsp.Mount(adapter, options.mountpoint, mountOptions...)
	if err != nil {
		return fmt.Errorf("mount WinFsp filesystem at %s: %w", options.mountpoint, err)
	}

	statusStop := make(chan struct{})
	go writeStatusLoop(session, options.statusFile, statusStop)
	<-ctx.Done()
	close(statusStop)
	filesystem.Unmount()
	drainContext, cancel := context.WithTimeout(context.Background(), options.drainTimeout)
	defer cancel()
	return session.Drain(drainContext)
}

type serviceHandler struct {
	configPath string
	debug      bool
	debugLog   string
}

func (h *serviceHandler) Execute(_ []string, requests <-chan svc.ChangeRequest, statuses chan<- svc.Status) (bool, uint32) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		options, err := optionsFromConfig(h.configPath, h.debug, h.debugLog)
		if err == nil {
			err = runMount(ctx, options)
		}
		result <- err
	}()
	accepts := svc.AcceptStop | svc.AcceptShutdown | svc.AcceptPreShutdown
	statuses <- svc.Status{State: svc.Running, Accepts: accepts}
	for {
		select {
		case request := <-requests:
			switch request.Cmd {
			case svc.Interrogate:
				statuses <- request.CurrentStatus
			case svc.Stop, svc.Shutdown, svc.PreShutdown:
				statuses <- svc.Status{State: svc.StopPending, WaitHint: 120_000}
				cancel()
				if err := <-result; err != nil {
					return true, 1
				}
				return false, 0
			}
		case err := <-result:
			if err != nil {
				return true, 1
			}
			return false, 0
		}
	}
}

type fileSystemAdapter struct {
	session *guestmount.Session
	debug   bool
}

func (a *fileSystemAdapter) OpenFile(name string, flag int, perm os.FileMode) (gofs.File, error) {
	file, err := a.session.OpenFile(name, flag, perm)
	if err != nil && a.debug {
		fmt.Fprintf(os.Stderr, "OpenFile name=%q flag=%#x error=%T %v\n", name, flag, err, err)
	}
	return file, err
}

func (a *fileSystemAdapter) Mkdir(name string, perm os.FileMode) error {
	return a.session.Mkdir(name, perm)
}

func (a *fileSystemAdapter) Stat(name string) (os.FileInfo, error) {
	return a.session.Stat(name)
}

func (a *fileSystemAdapter) Rename(source, target string) error {
	return a.session.Rename(source, target)
}

func (a *fileSystemAdapter) Remove(name string) error {
	return a.session.Remove(name)
}

func writeStatusLoop(session *guestmount.Session, statusFile string, stopping <-chan struct{}) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		status := session.Status()
		encoded, err := json.Marshal(status)
		if err == nil {
			_ = runtimeconfig.WriteFileAtomic(statusFile, encoded)
		}
		select {
		case <-stopping:
			return
		case <-ticker.C:
		}
	}
}
