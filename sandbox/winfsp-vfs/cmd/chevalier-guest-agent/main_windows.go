//go:build windows

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/control"
	"github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/runtimeconfig"
	"github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/serviceinstall"
	"golang.org/x/sys/windows/svc"
)

const serviceName = "ChevalierGuest"

func main() {
	configPath := flag.String("config", runtimeconfig.DefaultConfigPath, "runtime configuration path")
	installServices := flag.Bool("install-services", false, "install or update the Chevalier Windows services")
	flag.Parse()
	if *installServices {
		executable, err := os.Executable()
		if err != nil {
			fatal(fmt.Errorf("resolve guest agent path: %w", err))
		}
		if err := serviceinstall.Install(filepath.Dir(executable), *configPath); err != nil {
			fatal(err)
		}
		return
	}
	isService, err := svc.IsWindowsService()
	if err != nil {
		fatal(err)
	}
	if isService {
		if err := svc.Run(serviceName, &serviceHandler{configPath: *configPath}); err != nil {
			fatal(err)
		}
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := control.NewRuntime(*configPath).Run(ctx); err != nil {
		fatal(err)
	}
}

type serviceHandler struct {
	configPath string
}

func (h *serviceHandler) Execute(_ []string, requests <-chan svc.ChangeRequest, statuses chan<- svc.Status) (bool, uint32) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- control.NewRuntime(h.configPath).Run(ctx) }()
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

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
