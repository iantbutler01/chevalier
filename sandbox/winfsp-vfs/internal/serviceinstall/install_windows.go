//go:build windows

package serviceinstall

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc/mgr"
)

const preshutdownTimeout = 120 * time.Second

type definition struct {
	name        string
	displayName string
	description string
	executable  string
	startType   uint32
}

func Install(artifactDirectory, configPath string) error {
	manager, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("connect to service control manager: %w", err)
	}
	defer manager.Disconnect()

	definitions := []definition{
		{
			name:        "ChevalierVFS",
			displayName: "Chevalier VFS",
			description: "Mounts the authoritative OpenBracket workspace through WinFsp",
			executable:  filepath.Join(artifactDirectory, "chevalier-vfs-winfsp.exe"),
			startType:   mgr.StartManual,
		},
		{
			name:        "ChevalierGuest",
			displayName: "Chevalier Guest Control",
			description: "Authenticated command and lifecycle control for OpenBracket",
			executable:  filepath.Join(artifactDirectory, "chevalier-guest-agent.exe"),
			startType:   mgr.StartAutomatic,
		},
	}
	for _, service := range definitions {
		if err := installService(manager, service, configPath); err != nil {
			return err
		}
	}
	return nil
}

func installService(manager *mgr.Mgr, definition definition, configPath string) error {
	config := mgr.Config{
		ServiceType:    windows.SERVICE_WIN32_OWN_PROCESS,
		StartType:      definition.startType,
		ErrorControl:   mgr.ErrorNormal,
		BinaryPathName: syscall.EscapeArg(definition.executable) + " --config " + syscall.EscapeArg(configPath),
		DisplayName:    definition.displayName,
		Description:    definition.description,
	}
	service, err := manager.OpenService(definition.name)
	if errors.Is(err, windows.ERROR_SERVICE_DOES_NOT_EXIST) {
		service, err = manager.CreateService(
			definition.name,
			definition.executable,
			config,
			"--config",
			configPath,
		)
	} else if err == nil {
		err = service.UpdateConfig(config)
	}
	if err != nil {
		return fmt.Errorf("create or update service %s: %w", definition.name, err)
	}
	defer service.Close()

	recovery := []mgr.RecoveryAction{
		{Type: mgr.ServiceRestart, Delay: 5 * time.Second},
		{Type: mgr.ServiceRestart, Delay: 5 * time.Second},
		{Type: mgr.ServiceRestart, Delay: 5 * time.Second},
	}
	if err := service.SetRecoveryActions(recovery, 24*60*60); err != nil {
		return fmt.Errorf("set recovery actions for %s: %w", definition.name, err)
	}
	if err := service.SetRecoveryActionsOnNonCrashFailures(true); err != nil {
		return fmt.Errorf("enable recovery actions for %s: %w", definition.name, err)
	}
	timeout := struct{ Milliseconds uint32 }{Milliseconds: uint32(preshutdownTimeout / time.Millisecond)}
	if err := windows.ChangeServiceConfig2(
		service.Handle,
		windows.SERVICE_CONFIG_PRESHUTDOWN_INFO,
		(*byte)(unsafe.Pointer(&timeout)),
	); err != nil {
		return fmt.Errorf("set pre-shutdown timeout for %s: %w", definition.name, err)
	}
	return nil
}
