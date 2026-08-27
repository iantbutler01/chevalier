package mount

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const identityFileName = "mount-identity-v1"

func EnsureIdentity(stateRoot, identity string) error {
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return fmt.Errorf("mount identity is required")
	}
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		return fmt.Errorf("create state root: %w", err)
	}
	path := filepath.Join(stateRoot, identityFileName)
	existing, err := os.ReadFile(path)
	if err == nil {
		if strings.TrimSpace(string(existing)) != identity {
			return fmt.Errorf("VFS state belongs to a different VM or gateway scope")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("read mount identity: %w", err)
	}
	entries, err := os.ReadDir(stateRoot)
	if err != nil {
		return fmt.Errorf("inspect state root: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("refusing to bind non-empty VFS state without an identity")
	}
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create mount identity: %w", err)
	}
	removeTemporary := true
	defer func() {
		file.Close()
		if removeTemporary {
			os.Remove(temporary)
		}
	}()
	if _, err := file.WriteString(identity + "\n"); err != nil {
		return fmt.Errorf("write mount identity: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync mount identity: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close mount identity: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("publish mount identity: %w", err)
	}
	removeTemporary = false
	return nil
}
