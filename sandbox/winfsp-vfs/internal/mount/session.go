package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/gateway"
	"github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/journal"
)

type Session struct {
	operationMu sync.Mutex
	treeRoot    string
	stateRoot   string
	client      *gateway.Client
	journal     *journal.Journal
	stateLock   *stateLock
	notify      chan struct{}
	stopping    chan struct{}
	done        chan struct{}
	started     atomic.Bool
}

func Open(ctx context.Context, stateRoot string, client *gateway.Client) (*Session, error) {
	if client == nil {
		return nil, fmt.Errorf("gateway client is required")
	}
	stateRoot, err := filepath.Abs(stateRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve mount state root: %w", err)
	}
	treeRoot := filepath.Join(stateRoot, "tree")
	if err := os.MkdirAll(treeRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create materialized tree: %w", err)
	}
	stateLock, err := acquireStateLock(filepath.Join(stateRoot, "owner.lock"))
	if err != nil {
		return nil, err
	}
	wal, err := journal.Open(filepath.Join(stateRoot, "wal"))
	if err != nil {
		stateLock.Close()
		return nil, err
	}
	session := &Session{
		treeRoot:  treeRoot,
		stateRoot: stateRoot,
		client:    client,
		journal:   wal,
		stateLock: stateLock,
		notify:    make(chan struct{}, 1),
		stopping:  make(chan struct{}),
		done:      make(chan struct{}),
	}
	if err := session.hydrateIfFresh(ctx); err != nil {
		stateLock.Close()
		return nil, err
	}
	return session, nil
}

func (s *Session) Close() error {
	if s.started.Load() {
		s.StopPublisher()
	}
	return s.stateLock.Close()
}

func (s *Session) StartPublisher() {
	if !s.started.CompareAndSwap(false, true) {
		return
	}
	go s.publisherLoop()
	s.wakePublisher()
}

func (s *Session) StopPublisher() {
	if !s.started.Load() {
		return
	}
	select {
	case <-s.stopping:
	default:
		close(s.stopping)
	}
	<-s.done
}

func (s *Session) Drain(ctx context.Context) error {
	s.wakePublisher()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		status := s.journal.Status()
		if status.PendingEvents == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("drain %d pending WAL events: %w", status.PendingEvents, ctx.Err())
		case <-ticker.C:
			s.wakePublisher()
		}
	}
}

func (s *Session) Status() journal.Status {
	return s.journal.Status()
}

func (s *Session) OpenFile(name string, flag int, perm os.FileMode) (*TrackedFile, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	relative, local, err := s.resolve(name)
	if err != nil {
		return nil, err
	}
	_, statErr := os.Lstat(local)
	existed := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, statErr
	}
	file, err := openFileAtRoot(s.treeRoot, relative, local, flag, perm)
	if err != nil {
		return nil, normalizeLookupError(local, err)
	}
	tracked := &TrackedFile{File: file, session: s, relative: relative}
	if !existed && flag&os.O_CREATE != 0 {
		tracked.dirty.Store(true)
	}
	if existed && flag&os.O_TRUNC != 0 {
		tracked.dirty.Store(true)
	}
	return tracked, nil
}

func openFileAtRoot(root, relative, absolute string, flag int, perm os.FileMode) (*os.File, error) {
	if relative == "" {
		return os.OpenFile(absolute, flag, perm)
	}
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer rootHandle.Close()
	return rootHandle.OpenFile(relative, flag, perm)
}

func (s *Session) Mkdir(name string, perm os.FileMode) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	relative, local, err := s.resolve(name)
	if err != nil {
		return err
	}
	if err := os.Mkdir(local, perm); err != nil {
		return err
	}
	_, err = s.append(journal.Event{Kind: "create_directory", Path: relative, Mode: uint32(perm.Perm())}, "")
	return err
}

func (s *Session) Remove(name string) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	relative, local, err := s.resolve(name)
	if err != nil {
		return err
	}
	info, err := os.Lstat(local)
	if err != nil {
		return err
	}
	if err := os.Remove(local); err != nil {
		return err
	}
	kind := "delete_file"
	if info.IsDir() {
		kind = "remove_directory"
	}
	_, err = s.append(journal.Event{Kind: kind, Path: relative}, "")
	return err
}

func (s *Session) Rename(source, target string) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	from, localSource, err := s.resolve(source)
	if err != nil {
		return err
	}
	to, localTarget, err := s.resolveNew(target)
	if err != nil {
		return err
	}
	if err := os.Rename(localSource, localTarget); err != nil {
		return err
	}
	_, err = s.append(journal.Event{Kind: "rename", From: from, To: to, Path: to}, "")
	return err
}

func (s *Session) Stat(name string) (os.FileInfo, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	_, local, err := s.resolve(name)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(local)
	if err != nil {
		return nil, normalizeLookupError(local, err)
	}
	return info, nil
}

func (s *Session) seal(file *TrackedFile) error {
	if !file.dirty.CompareAndSwap(true, false) {
		return nil
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if err := file.File.Sync(); err != nil {
		file.dirty.Store(true)
		return err
	}
	info, err := file.File.Stat()
	if err != nil {
		file.dirty.Store(true)
		return err
	}
	event := journal.Event{Kind: "replace_file", Path: file.relative, Mode: uint32(info.Mode().Perm())}
	if _, err := s.append(event, file.File.Name()); err != nil {
		file.dirty.Store(true)
		return err
	}
	return nil
}

func (s *Session) append(event journal.Event, payloadPath string) (journal.Event, error) {
	committed, err := s.journal.Append(event, payloadPath)
	if err == nil {
		s.wakePublisher()
	}
	return committed, err
}

func (s *Session) resolve(name string) (string, string, error) {
	return s.resolveWithFinal(name, true)
}

func (s *Session) resolveNew(name string) (string, string, error) {
	return s.resolveWithFinal(name, false)
}

func (s *Session) resolveWithFinal(name string, canonicalizeFinal bool) (string, string, error) {
	relative, err := journal.ValidateRelativePath(name)
	if err != nil {
		return "", "", err
	}
	components := strings.Split(relative, "/")
	current := s.treeRoot
	for index, component := range components {
		if component == "" || (!canonicalizeFinal && index == len(components)-1) {
			current = filepath.Join(current, component)
			continue
		}
		entries, readErr := os.ReadDir(current)
		if readErr != nil {
			if os.IsNotExist(readErr) {
				current = filepath.Join(current, component)
				continue
			}
			return "", "", readErr
		}
		matched := ""
		for _, entry := range entries {
			if strings.EqualFold(entry.Name(), component) {
				if matched != "" && matched != entry.Name() {
					return "", "", fmt.Errorf("case-colliding VFS entries %q and %q", matched, entry.Name())
				}
				matched = entry.Name()
			}
		}
		if matched != "" {
			components[index] = matched
		}
		current = filepath.Join(current, components[index])
	}
	relative = strings.Join(components, "/")
	local := s.treeRoot
	if relative != "" {
		local = filepath.Join(append([]string{s.treeRoot}, strings.Split(relative, "/")...)...)
	}
	resolved, err := filepath.Abs(local)
	if err != nil {
		return "", "", err
	}
	rel, err := filepath.Rel(s.treeRoot, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("VFS path escapes materialized root: %q", name)
	}
	return relative, resolved, nil
}

func (s *Session) hydrateIfFresh(ctx context.Context) error {
	marker := filepath.Join(s.stateRoot, "hydrated-v1")
	if _, err := os.Stat(marker); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat hydration marker: %w", err)
	}
	if !s.journal.Empty() {
		return fmt.Errorf("refusing to hydrate over an existing WAL")
	}
	entries, err := os.ReadDir(s.treeRoot)
	if err != nil {
		return fmt.Errorf("read fresh materialized tree: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("refusing to hydrate over a non-empty materialized tree")
	}
	if err := s.hydrateDirectory(ctx, ""); err != nil {
		return err
	}
	markerFile, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create hydration marker: %w", err)
	}
	if err := markerFile.Sync(); err != nil {
		markerFile.Close()
		return fmt.Errorf("sync hydration marker: %w", err)
	}
	return markerFile.Close()
}

func (s *Session) hydrateDirectory(ctx context.Context, relative string) error {
	entries, err := s.client.ListDirectory(ctx, relative)
	if err != nil {
		return fmt.Errorf("hydrate directory %q: %w", relative, err)
	}
	for _, entry := range entries {
		child := entry.Name
		if relative != "" {
			child = relative + "/" + entry.Name
		}
		canonicalChild, local, err := s.resolve(child)
		if err != nil {
			return err
		}
		if canonicalChild != child {
			return fmt.Errorf("case-colliding VFS entries %q and %q", canonicalChild, child)
		}
		switch entry.Kind {
		case "directory":
			if err := os.Mkdir(local, 0o700); err != nil {
				return fmt.Errorf("hydrate directory %q: %w", child, err)
			}
			if err := s.hydrateDirectory(ctx, child); err != nil {
				return err
			}
		case "file":
			body, err := s.client.ReadFile(ctx, child)
			if err != nil {
				return fmt.Errorf("hydrate file %q: %w", child, err)
			}
			file, err := os.OpenFile(local, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return fmt.Errorf("create hydrated file %q: %w", child, err)
			}
			if _, err := file.Write(body); err != nil {
				file.Close()
				return fmt.Errorf("write hydrated file %q: %w", child, err)
			}
			if err := file.Close(); err != nil {
				return fmt.Errorf("materialize file %q: %w", child, err)
			}
		default:
			return fmt.Errorf("Windows guest VFS does not support hydrated %s entry %q", entry.Kind, child)
		}
	}
	return nil
}

func (s *Session) publisherLoop() {
	defer close(s.done)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopping:
			return
		case <-s.notify:
		case <-ticker.C:
		}
		s.publishPending()
	}
}

func (s *Session) publishPending() {
	for _, event := range s.journal.Pending() {
		payload, err := s.journal.ReadPayload(event)
		if err == nil {
			_, err = s.client.Publish(context.Background(), gateway.Event{
				Sequence:    event.Sequence,
				OperationID: s.journal.OperationID(event.Sequence),
				Kind:        event.Kind,
				Path:        event.Path,
				From:        event.From,
				To:          event.To,
				Mode:        event.Mode,
				Payload:     payload,
			})
		}
		if err != nil {
			s.journal.SetLastError(err)
			return
		}
		if err := s.journal.Acknowledge(event.Sequence); err != nil {
			s.journal.SetLastError(err)
			return
		}
		s.journal.SetLastError(nil)
	}
}

func (s *Session) wakePublisher() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

type TrackedFile struct {
	*os.File
	session  *Session
	relative string
	dirty    atomic.Bool
	closed   atomic.Bool
}

func (f *TrackedFile) Write(bytes []byte) (int, error) {
	count, err := f.File.Write(bytes)
	if count != 0 {
		f.dirty.Store(true)
	}
	return count, err
}

func (f *TrackedFile) WriteAt(bytes []byte, offset int64) (int, error) {
	count, err := f.File.WriteAt(bytes, offset)
	if count != 0 {
		f.dirty.Store(true)
	}
	return count, err
}

func (f *TrackedFile) Truncate(size int64) error {
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	f.dirty.Store(true)
	return nil
}

func (f *TrackedFile) Sync() error {
	return f.session.seal(f)
}

func (f *TrackedFile) Close() error {
	if !f.closed.CompareAndSwap(false, true) {
		return os.ErrClosed
	}
	sealErr := f.session.seal(f)
	closeErr := f.File.Close()
	return errors.Join(sealErr, closeErr)
}

func (f *TrackedFile) Append(bytes []byte) (int, error) {
	count, err := f.File.Write(bytes)
	if count != 0 {
		f.dirty.Store(true)
	}
	return count, err
}

func (f *TrackedFile) ConstrainedWriteAt(bytes []byte, offset int64) (int, error) {
	info, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	if offset >= info.Size() {
		return 0, nil
	}
	if end := offset + int64(len(bytes)); end > info.Size() {
		bytes = bytes[:info.Size()-offset]
	}
	return f.WriteAt(bytes, offset)
}

func (f *TrackedFile) Shrink(size int64) error {
	info, err := f.File.Stat()
	if err != nil {
		return err
	}
	if info.Size() <= size {
		return nil
	}
	return f.Truncate(size)
}

var _ io.ReadWriteCloser = (*TrackedFile)(nil)
