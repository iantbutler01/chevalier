//go:build windows

package runtimeconfig

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	seedConfigFile  = "RUNTIME.JSN"
	seedControlFile = "CTRL.TKN"
	seedVFSFile     = "VFS.TKN"
)

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
	if err := scrubFirstBootCredentials(); err != nil {
		return Config{}, err
	}
	return config, nil
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
	} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove cached first-boot answer file %s: %w", path, err)
		}
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
		_ = os.Remove(temporary)
		return fmt.Errorf("replace runtime config file: %w", err)
	}
	removeTemporary = false
	return nil
}
