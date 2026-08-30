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
	seedDesktopFile = "BOOT.TKN"
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
	desktopToken, err := readSeedToken(filepath.Join(root, seedDesktopFile))
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
		DefaultDesktopToken: desktopToken,
	} {
		if err := WriteFileAtomic(path, data); err != nil {
			return Config{}, err
		}
	}
	return config, nil
}

func FinalizeFirstBoot(ctx context.Context) error {
	_, found, err := findSeedRoot()
	if err != nil {
		return err
	}
	if !found {
		if _, err := os.Stat(DefaultDesktopToken); err == nil {
			found = true
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect installed desktop login token: %w", err)
		}
	}
	if !found {
		return nil
	}
	if err := prepareDesktopAccount(); err != nil {
		return err
	}
	for {
		complete, err := setupComplete(ctx)
		if err != nil {
			return err
		}
		if complete {
			break
		}
		setupFinished, err := windowsSetupFinished()
		if err != nil {
			return err
		}
		if setupFinished {
			if _, err := os.Stat(DefaultFirstBootRestart); os.IsNotExist(err) {
				if err := configureDesktopAutologon(); err != nil {
					return err
				}
				if err := WriteFileAtomic(DefaultFirstBootRestart, []byte("requested\n")); err != nil {
					return fmt.Errorf("record desktop restart request: %w", err)
				}
				complete, err := waitForDesktopSession(ctx, 30*time.Second)
				if err != nil {
					return err
				}
				if complete {
					break
				}
				if output, err := exec.CommandContext(ctx, "shutdown.exe", "/r", "/t", "0", "/f").CombinedOutput(); err != nil {
					return fmt.Errorf("restart for OpenBracket desktop login: %w: %s", err, strings.TrimSpace(string(output)))
				}
				<-ctx.Done()
				return ctx.Err()
			} else if err != nil {
				return fmt.Errorf("inspect desktop restart request: %w", err)
			}
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
	return nil
}

func waitForDesktopSession(ctx context.Context, timeout time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		complete, err := setupComplete(ctx)
		if err != nil {
			return false, err
		}
		if complete {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return false, nil
}

func windowsSetupFinished() (bool, error) {
	statePath, err := windows.UTF16PtrFromString(`SOFTWARE\Microsoft\Windows\CurrentVersion\Setup\State`)
	if err != nil {
		return false, fmt.Errorf("encode Windows setup state registry path: %w", err)
	}
	var stateKey windows.Handle
	if err := windows.RegOpenKeyEx(windows.HKEY_LOCAL_MACHINE, statePath, 0, windows.KEY_QUERY_VALUE, &stateKey); err != nil {
		return false, fmt.Errorf("open Windows setup state registry path: %w", err)
	}
	imageState, err := registryString(stateKey, "ImageState")
	windows.RegCloseKey(stateKey)
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(imageState, "IMAGE_STATE_COMPLETE") {
		return false, nil
	}

	path, err := windows.UTF16PtrFromString(`SYSTEM\Setup`)
	if err != nil {
		return false, fmt.Errorf("encode Windows setup registry path: %w", err)
	}
	var key windows.Handle
	if err := windows.RegOpenKeyEx(windows.HKEY_LOCAL_MACHINE, path, 0, windows.KEY_QUERY_VALUE, &key); err != nil {
		return false, fmt.Errorf("open Windows setup registry path: %w", err)
	}
	defer windows.RegCloseKey(key)
	for _, name := range []string{"SystemSetupInProgress", "OOBEInProgress", "SetupType", "SetupPhase"} {
		value, err := registryDWORD(key, name)
		if err != nil {
			return false, err
		}
		if value != 0 {
			return false, nil
		}
	}
	return true, nil
}

func prepareDesktopAccount() error {
	passwordBytes, err := os.ReadFile(DefaultDesktopToken)
	if err != nil {
		return fmt.Errorf("read desktop login token: %w", err)
	}
	password := strings.TrimSpace(string(passwordBytes))
	if password == "" {
		return fmt.Errorf("desktop login token is empty")
	}
	username, err := windows.UTF16PtrFromString("OpenBracket")
	if err != nil {
		return fmt.Errorf("encode desktop account name: %w", err)
	}
	encodedPassword, err := windows.UTF16PtrFromString(password)
	if err != nil {
		return fmt.Errorf("encode desktop account password: %w", err)
	}
	comment, err := windows.UTF16PtrFromString("OpenBracket desktop user")
	if err != nil {
		return fmt.Errorf("encode desktop account description: %w", err)
	}
	user := struct {
		Name        *uint16
		Password    *uint16
		PasswordAge uint32
		Privilege   uint32
		HomeDir     *uint16
		Comment     *uint16
		Flags       uint32
		ScriptPath  *uint16
	}{
		Name:      username,
		Password:  encodedPassword,
		Privilege: 1,
		Comment:   comment,
		Flags:     0x0001 | 0x0200 | 0x10000,
	}
	var parameterError uint32
	status, _, _ := windows.NewLazySystemDLL("netapi32.dll").NewProc("NetUserAdd").Call(
		0,
		1,
		uintptr(unsafe.Pointer(&user)),
		uintptr(unsafe.Pointer(&parameterError)),
	)
	if status != 0 && status != 2224 {
		return fmt.Errorf("create desktop account: Windows status %d at parameter %d", status, parameterError)
	}
	passwordInfo := struct {
		Password *uint16
	}{Password: encodedPassword}
	if err := setUserInfo(username, 1003, unsafe.Pointer(&passwordInfo)); err != nil {
		return fmt.Errorf("set desktop account password: %w", err)
	}
	flagsInfo := struct {
		Flags uint32
	}{Flags: 0x0001 | 0x0200 | 0x10000}
	if err := setUserInfo(username, 1008, unsafe.Pointer(&flagsInfo)); err != nil {
		return fmt.Errorf("enable desktop account: %w", err)
	}
	return ensureLocalGroupMember("Users", "OpenBracket")
}

func ensureLocalGroupMember(groupName string, username string) error {
	encodedGroup, err := windows.UTF16PtrFromString(groupName)
	if err != nil {
		return fmt.Errorf("encode local group name: %w", err)
	}
	encodedUsername, err := windows.UTF16PtrFromString(username)
	if err != nil {
		return fmt.Errorf("encode local group member: %w", err)
	}
	member := struct {
		DomainAndName *uint16
	}{DomainAndName: encodedUsername}
	status, _, _ := windows.NewLazySystemDLL("netapi32.dll").NewProc("NetLocalGroupAddMembers").Call(
		0,
		uintptr(unsafe.Pointer(encodedGroup)),
		3,
		uintptr(unsafe.Pointer(&member)),
		1,
	)
	if status != 0 && status != 1378 {
		return fmt.Errorf("add desktop account to %s: Windows status %d", groupName, status)
	}
	return nil
}

func setUserInfo(username *uint16, level uint32, info unsafe.Pointer) error {
	var parameterError uint32
	status, _, _ := windows.NewLazySystemDLL("netapi32.dll").NewProc("NetUserSetInfo").Call(
		0,
		uintptr(unsafe.Pointer(username)),
		uintptr(level),
		uintptr(info),
		uintptr(unsafe.Pointer(&parameterError)),
	)
	if status != 0 {
		return fmt.Errorf("Windows status %d at parameter %d", status, parameterError)
	}
	return nil
}

func configureDesktopAutologon() error {
	passwordBytes, err := os.ReadFile(DefaultDesktopToken)
	if err != nil {
		return fmt.Errorf("read desktop login token: %w", err)
	}
	password := strings.TrimSpace(string(passwordBytes))
	if password == "" {
		return fmt.Errorf("desktop login token is empty")
	}
	computerName, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("resolve Windows computer name: %w", err)
	}
	path, err := windows.UTF16PtrFromString(`SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon`)
	if err != nil {
		return fmt.Errorf("encode Winlogon registry path: %w", err)
	}
	var key windows.Handle
	if err := windows.RegOpenKeyEx(windows.HKEY_LOCAL_MACHINE, path, 0, windows.KEY_SET_VALUE, &key); err != nil {
		return fmt.Errorf("open Winlogon registry path: %w", err)
	}
	defer windows.RegCloseKey(key)
	for name, value := range map[string]string{
		"AutoAdminLogon":    "1",
		"DefaultDomainName": computerName,
		"DefaultPassword":   password,
		"DefaultUserName":   "OpenBracket",
	} {
		if err := setRegistryString(key, name, value); err != nil {
			return err
		}
	}
	return setRegistryDWORD(key, "AutoLogonCount", 1)
}

func registryDWORD(key windows.Handle, name string) (uint32, error) {
	encodedName, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, fmt.Errorf("encode registry value name %s: %w", name, err)
	}
	var valueType uint32
	var value uint32
	valueSize := uint32(unsafe.Sizeof(value))
	if err := windows.RegQueryValueEx(key, encodedName, nil, &valueType, (*byte)(unsafe.Pointer(&value)), &valueSize); err != nil {
		return 0, fmt.Errorf("read registry value %s: %w", name, err)
	}
	if valueType != windows.REG_DWORD || valueSize != uint32(unsafe.Sizeof(value)) {
		return 0, fmt.Errorf("registry value %s is not a DWORD", name)
	}
	return value, nil
}

func registryString(key windows.Handle, name string) (string, error) {
	encodedName, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return "", fmt.Errorf("encode registry value name %s: %w", name, err)
	}
	var valueType uint32
	var valueSize uint32
	if err := windows.RegQueryValueEx(key, encodedName, nil, &valueType, nil, &valueSize); err != nil {
		return "", fmt.Errorf("size registry value %s: %w", name, err)
	}
	if valueType != windows.REG_SZ && valueType != windows.REG_EXPAND_SZ {
		return "", fmt.Errorf("registry value %s is not a string", name)
	}
	value := make([]uint16, (valueSize+1)/2)
	if err := windows.RegQueryValueEx(key, encodedName, nil, &valueType, (*byte)(unsafe.Pointer(&value[0])), &valueSize); err != nil {
		return "", fmt.Errorf("read registry value %s: %w", name, err)
	}
	return windows.UTF16ToString(value), nil
}

func setRegistryString(key windows.Handle, name string, value string) error {
	encodedName, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return fmt.Errorf("encode registry value name %s: %w", name, err)
	}
	encodedValue, err := windows.UTF16FromString(value)
	if err != nil {
		return fmt.Errorf("encode registry value %s: %w", name, err)
	}
	return setRegistryValue(key, encodedName, windows.REG_SZ, unsafe.Pointer(&encodedValue[0]), uint32(len(encodedValue)*2), name)
}

func setRegistryDWORD(key windows.Handle, name string, value uint32) error {
	encodedName, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return fmt.Errorf("encode registry value name %s: %w", name, err)
	}
	return setRegistryValue(key, encodedName, windows.REG_DWORD, unsafe.Pointer(&value), uint32(unsafe.Sizeof(value)), name)
}

func setRegistryValue(key windows.Handle, name *uint16, valueType uint32, value unsafe.Pointer, valueSize uint32, label string) error {
	status, _, _ := windows.NewLazySystemDLL("advapi32.dll").NewProc("RegSetValueExW").Call(
		uintptr(key),
		uintptr(unsafe.Pointer(name)),
		0,
		uintptr(valueType),
		uintptr(value),
		uintptr(valueSize),
	)
	if status != 0 {
		return fmt.Errorf("set registry value %s: Windows status %d", label, status)
	}
	return nil
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
	output, err := exec.CommandContext(ctx, "query.exe", "user").CombinedOutput()
	if err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			return false, fmt.Errorf("query OpenBracket desktop session: %w", err)
		}
	}
	if !hasDesktopSession(string(output), "OpenBracket") {
		return false, nil
	}
	if err := exec.CommandContext(ctx, "net.exe", "user", "OpenBracket").Run(); err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return false, nil
		}
		return false, fmt.Errorf("query OpenBracket desktop account: %w", err)
	}
	if _, err := os.Stat(`C:\Users\OpenBracket\NTUSER.DAT`); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("inspect OpenBracket desktop profile: %w", err)
	}
	return true, nil
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
	winlogonPath, err := windows.UTF16PtrFromString(`SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon`)
	if err != nil {
		return fmt.Errorf("encode Winlogon registry path: %w", err)
	}
	var winlogon windows.Handle
	if err := windows.RegOpenKeyEx(windows.HKEY_LOCAL_MACHINE, winlogonPath, 0, windows.KEY_SET_VALUE, &winlogon); err != nil {
		return fmt.Errorf("open Winlogon registry path: %w", err)
	}
	defer windows.RegCloseKey(winlogon)
	deleteValue := windows.NewLazySystemDLL("advapi32.dll").NewProc("RegDeleteValueW")
	for _, value := range []string{
		"AutoAdminLogon",
		"AutoLogonCount",
		"DefaultDomainName",
		"DefaultPassword",
		"DefaultUserName",
	} {
		name, err := windows.UTF16PtrFromString(value)
		if err != nil {
			return fmt.Errorf("encode Winlogon value %s: %w", value, err)
		}
		status, _, _ := deleteValue.Call(uintptr(winlogon), uintptr(unsafe.Pointer(name)))
		if status != 0 && status != uintptr(windows.ERROR_FILE_NOT_FOUND) {
			return fmt.Errorf("clear first-boot Winlogon value %s: Windows status %d", value, status)
		}
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
	for _, path := range []string{DefaultDesktopToken, DefaultFirstBootRestart} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove first-boot credential state %s: %w", path, err)
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
		return fmt.Errorf("query embedded first-boot answer registration: %w", err)
	}
	if output, err := exec.Command(
		"reg.exe",
		"delete",
		`HKLM\SYSTEM\Setup`,
		"/v",
		"UnattendFile",
		"/f",
	).CombinedOutput(); err != nil {
		return fmt.Errorf("clear embedded first-boot answer registration: %w: %s", err, strings.TrimSpace(string(output)))
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
