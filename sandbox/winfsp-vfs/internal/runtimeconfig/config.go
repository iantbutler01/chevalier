package runtimeconfig

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	SchemaVersion           = 1
	DefaultInstalledDir     = `C:\ProgramData\Chevalier\runtime`
	DefaultConfigPath       = DefaultInstalledDir + `\runtime.json`
	DefaultControlToken     = DefaultInstalledDir + `\control.token`
	DefaultVFSToken         = DefaultInstalledDir + `\vfs.token`
	DefaultDesktopToken     = DefaultInstalledDir + `\desktop.token`
	DefaultFirstBootRestart = DefaultInstalledDir + `\desktop-restart.requested`
	DefaultGuestStatus      = `C:\ProgramData\Chevalier\guest-status.json`
	DefaultFirstBootStatus  = `C:\ProgramData\Chevalier\first-boot-status.json`
	DefaultStateRoot        = `C:\ProgramData\Chevalier\state-volume\workspace`
	DefaultStatusPath       = DefaultStateRoot + `\status.json`
	DefaultMountpoint       = `W:`
	DefaultListen           = `0.0.0.0:13338`
)

type Config struct {
	SchemaVersion int           `json:"schemaVersion"`
	VMID          string        `json:"vmId"`
	Generation    string        `json:"generation"`
	Control       ControlConfig `json:"control"`
	VFS           VFSConfig     `json:"vfs"`
}

type ControlConfig struct {
	ListenAddress string `json:"listenAddress"`
	TokenFile     string `json:"tokenFile"`
}

type VFSConfig struct {
	Endpoint       string `json:"endpoint"`
	Scope          string `json:"scope"`
	TokenFile      string `json:"tokenFile"`
	StateDirectory string `json:"stateDirectory"`
	Mountpoint     string `json:"mountpoint"`
	StatusFile     string `json:"statusFile"`
	DrainTimeout   string `json:"drainTimeout"`
}

func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read runtime config: %w", err)
	}
	var config Config
	if err := json.Unmarshal(raw, &config); err != nil {
		return Config{}, fmt.Errorf("decode runtime config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if c.SchemaVersion != SchemaVersion {
		return fmt.Errorf("runtime config schema must be %d", SchemaVersion)
	}
	if strings.TrimSpace(c.VMID) == "" || strings.TrimSpace(c.Generation) == "" {
		return fmt.Errorf("runtime config requires vmId and generation")
	}
	if _, _, err := net.SplitHostPort(c.Control.ListenAddress); err != nil {
		return fmt.Errorf("invalid control listen address: %w", err)
	}
	for label, value := range map[string]string{
		"control token file":  c.Control.TokenFile,
		"VFS endpoint":        c.VFS.Endpoint,
		"VFS scope":           c.VFS.Scope,
		"VFS token file":      c.VFS.TokenFile,
		"VFS state directory": c.VFS.StateDirectory,
		"VFS mountpoint":      c.VFS.Mountpoint,
		"VFS status file":     c.VFS.StatusFile,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("runtime config requires %s", label)
		}
	}
	if !isWindowsAbsolute(c.Control.TokenFile) || !isWindowsAbsolute(c.VFS.TokenFile) || !isWindowsAbsolute(c.VFS.StateDirectory) {
		return fmt.Errorf("runtime config token and state paths must be absolute")
	}
	return nil
}

func isWindowsAbsolute(path string) bool {
	return len(path) >= 3 && path[1] == ':' && (path[2] == '\\' || path[2] == '/')
}

func hasDesktopSession(output string, username string) bool {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(strings.TrimPrefix(strings.TrimSpace(line), ">"))
		if len(fields) > 0 && strings.EqualFold(fields[0], username) {
			return true
		}
	}
	return false
}
