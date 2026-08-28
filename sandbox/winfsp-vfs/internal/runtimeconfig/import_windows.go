//go:build windows

package runtimeconfig

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	seedConfigFile  = "RUNTIME.JSN"
	seedControlFile = "CTRL.TKN"
	seedVFSFile     = "VFS.TKN"
	seedAnswerFile  = "AUTOUNATTEND.XML"
	installedAnswer = `C:\Windows\Panther\unattend.xml`
	restartMarker   = `C:\ProgramData\Chevalier\runtime\first-boot-restart-scheduled`
	desktopProfile  = `C:\Users\OpenBracket\NTUSER.DAT`
	ioctlEjectMedia = 0x002d4808
)

var ErrFirstBootRestartScheduled = errors.New("Windows first-boot answer restart scheduled")

func ImportSeed() (Config, bool, error) {
	for letter := 'D'; letter <= 'Z'; letter++ {
		root := fmt.Sprintf("%c:\\", letter)
		configPath := filepath.Join(root, seedConfigFile)
		if _, err := os.Stat(configPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return Config{}, false, fmt.Errorf("inspect runtime seed %s: %w", root, err)
		}
		config, err := importSeedAt(root)
		if err != nil {
			return Config{}, false, err
		}
		return config, true, nil
	}
	return Config{}, false, nil
}

func importSeedAt(root string) (Config, error) {
	config, err := Load(filepath.Join(root, seedConfigFile))
	if err != nil {
		return Config{}, fmt.Errorf("load runtime seed: %w", err)
	}
	controlToken, err := readSeedToken(filepath.Join(root, seedControlFile))
	if err != nil {
		return Config{}, err
	}
	vfsToken, err := readSeedToken(filepath.Join(root, seedVFSFile))
	if err != nil {
		return Config{}, err
	}
	if err := os.MkdirAll(DefaultInstalledDir, 0o700); err != nil {
		return Config{}, fmt.Errorf("create runtime config directory: %w", err)
	}
	if output, err := exec.Command(
		"icacls.exe",
		DefaultInstalledDir,
		"/inheritance:r",
		"/grant:r",
		"*S-1-5-18:(OI)(CI)F",
		"*S-1-5-32-544:(OI)(CI)F",
	).CombinedOutput(); err != nil {
		return Config{}, fmt.Errorf("secure runtime config directory: %w: %s", err, strings.TrimSpace(string(output)))
	}
	configBytes, err := os.ReadFile(filepath.Join(root, seedConfigFile))
	if err != nil {
		return Config{}, fmt.Errorf("read runtime seed config: %w", err)
	}
	for path, data := range map[string][]byte{
		DefaultConfigPath:   configBytes,
		DefaultControlToken: controlToken,
		DefaultVFSToken:     vfsToken,
	} {
		if err := WriteFileAtomic(path, data); err != nil {
			return Config{}, err
		}
	}
	answer, err := os.ReadFile(filepath.Join(root, seedAnswerFile))
	if err != nil {
		return Config{}, fmt.Errorf("read runtime first-boot answer: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(installedAnswer), 0o700); err != nil {
		return Config{}, fmt.Errorf("create Windows answer directory: %w", err)
	}
	if err := WriteFileAtomic(installedAnswer, answer); err != nil {
		return Config{}, fmt.Errorf("install runtime first-boot answer: %w", err)
	}
	if output, err := exec.Command(
		"reg.exe",
		"add",
		`HKLM\SYSTEM\Setup`,
		"/v",
		"UnattendFile",
		"/t",
		"REG_SZ",
		"/d",
		installedAnswer,
		"/f",
	).CombinedOutput(); err != nil {
		return Config{}, fmt.Errorf("register runtime first-boot answer: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return config, nil
}

func FinalizeFirstBoot(ctx context.Context) error {
	root, found, err := findSeedRoot()
	if err != nil || !found {
		return err
	}
	complete, err := setupComplete(ctx)
	if err != nil {
		return err
	}
	if !complete {
		restartScheduled, err := scheduleFirstBootOOBE(ctx)
		if err != nil {
			return err
		}
		if restartScheduled {
			return ErrFirstBootRestartScheduled
		}
	}
	for {
		complete, err = setupComplete(ctx)
		if err != nil {
			return err
		}
		if complete {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if err := scrubFirstBootCredentials(); err != nil {
		return err
	}
	if err := ejectSeedVolume(root); err != nil {
		return fmt.Errorf("eject runtime first-boot media: %w", err)
	}
	return nil
}

func scheduleFirstBootOOBE(ctx context.Context) (bool, error) {
	if _, err := os.Stat(restartMarker); err == nil {
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("inspect Windows first-boot restart marker: %w", err)
	}
	if err := WriteFileAtomic(restartMarker, []byte("scheduled\n")); err != nil {
		return false, fmt.Errorf("write Windows first-boot restart marker: %w", err)
	}
	systemRoot := os.Getenv("SystemRoot")
	if systemRoot == "" {
		systemRoot = `C:\Windows`
	}
	output, err := exec.CommandContext(
		ctx,
		filepath.Join(systemRoot, "System32", "Sysprep", "Sysprep.exe"),
		"/oobe",
		"/reboot",
		"/quiet",
		"/unattend:"+installedAnswer,
	).CombinedOutput()
	if err != nil {
		_ = os.Remove(restartMarker)
		return false, fmt.Errorf("apply Windows first-boot answer with Sysprep: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return true, nil
}

func findSeedRoot() (string, bool, error) {
	for letter := 'D'; letter <= 'Z'; letter++ {
		root := fmt.Sprintf("%c:\\", letter)
		if _, err := os.Stat(filepath.Join(root, seedConfigFile)); err == nil {
			return root, true, nil
		} else if !os.IsNotExist(err) {
			return "", false, fmt.Errorf("inspect runtime seed %s: %w", root, err)
		}
	}
	return "", false, nil
}

func setupComplete(ctx context.Context) (bool, error) {
	output, err := exec.CommandContext(
		ctx,
		"reg.exe",
		"query",
		`HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Setup\State`,
		"/v",
		"ImageState",
	).CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("query Windows setup state: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if !strings.Contains(strings.ToUpper(string(output)), "IMAGE_STATE_COMPLETE") {
		return false, nil
	}
	if err := exec.CommandContext(ctx, "net.exe", "user", "OpenBracket").Run(); err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return false, nil
		}
		return false, fmt.Errorf("query OpenBracket desktop account: %w", err)
	}
	if _, err := os.Stat(desktopProfile); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("inspect OpenBracket desktop profile: %w", err)
	}
	return true, nil
}

func ejectSeedVolume(root string) error {
	device, err := windows.UTF16PtrFromString(`\\.\` + strings.TrimSuffix(root, `\`))
	if err != nil {
		return fmt.Errorf("encode runtime media device: %w", err)
	}
	handle, err := windows.CreateFile(
		device,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		0,
		0,
	)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	var returned uint32
	return windows.DeviceIoControl(handle, ioctlEjectMedia, nil, 0, nil, 0, &returned, nil)
}

func scrubFirstBootCredentials() error {
	username, err := windows.UTF16PtrFromString("OpenBracketBootstrap")
	if err != nil {
		return fmt.Errorf("encode bootstrap account name: %w", err)
	}
	status, _, _ := windows.NewLazySystemDLL("netapi32.dll").NewProc("NetUserDel").Call(0, uintptr(unsafe.Pointer(username)))
	if status != 0 && status != 2221 {
		return fmt.Errorf("delete bootstrap account: Windows status %d", status)
	}
	for _, path := range []string{
		`C:\Windows\Panther\unattend.xml`,
		`C:\Windows\Panther\unattend-original.xml`,
		`C:\Windows\Panther\Autounattend.xml`,
		`C:\Windows\Panther\Unattend\unattend.xml`,
		restartMarker,
	} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove cached first-boot answer file %s: %w", path, err)
		}
	}
	if err := exec.Command(
		"reg.exe",
		"query",
		`HKLM\SYSTEM\Setup`,
		"/v",
		"UnattendFile",
	).Run(); err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return nil
		}
		return fmt.Errorf("query runtime first-boot answer registration: %w", err)
	}
	if output, err := exec.Command(
		"reg.exe",
		"delete",
		`HKLM\SYSTEM\Setup`,
		"/v",
		"UnattendFile",
		"/f",
	).CombinedOutput(); err != nil {
		return fmt.Errorf("clear runtime first-boot answer registration: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func readSeedToken(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read runtime seed token: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" || len(token) > 8192 {
		return nil, fmt.Errorf("runtime seed token is empty or too large")
	}
	return []byte(token + "\n"), nil
}

func WriteFileAtomic(path string, data []byte) error {
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("write runtime config temporary file: %w", err)
	}
	removeTemporary := true
	defer func() {
		file.Close()
		if removeTemporary {
			os.Remove(temporary)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write runtime config temporary file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync runtime config temporary file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close runtime config temporary file: %w", err)
	}
	from, err := windows.UTF16PtrFromString(temporary)
	if err != nil {
		return fmt.Errorf("encode runtime config temporary path: %w", err)
	}
	to, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return fmt.Errorf("encode runtime config path: %w", err)
	}
	if err := windows.MoveFileEx(
		from,
		to,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	); err != nil {
		if err != windows.ERROR_INVALID_PARAMETER {
			_ = os.Remove(temporary)
			return fmt.Errorf("replace runtime config file: %w", err)
		}
		// WinFsp implements atomic replace but rejects MoveFileEx's optional
		// write-through hint. The temporary file was flushed above and the mount
		// journals the rename, so retry only that capability mismatch.
		if err := windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING); err != nil {
			_ = os.Remove(temporary)
			return fmt.Errorf("replace runtime config file without write-through: %w", err)
		}
	}
	removeTemporary = false
	return nil
}
