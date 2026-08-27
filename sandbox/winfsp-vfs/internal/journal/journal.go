package journal

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Event struct {
	Sequence uint64 `json:"sequence"`
	Kind     string `json:"kind"`
	Path     string `json:"path,omitempty"`
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
	Mode     uint32 `json:"mode,omitempty"`
	Payload  string `json:"payload,omitempty"`
}

type Status struct {
	AcknowledgedSequence  uint64 `json:"acknowledged_sequence"`
	LastCommittedSequence uint64 `json:"last_committed_sequence"`
	PendingEvents         uint64 `json:"pending_events"`
	LastError             string `json:"last_error,omitempty"`
}

type Journal struct {
	mu      sync.Mutex
	root    string
	logPath string
	epoch   string
	events  []Event
	ack     uint64
	lastErr string
}

func Open(root string) (*Journal, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve journal root: %w", err)
	}
	for _, directory := range []string{root, filepath.Join(root, "payloads"), filepath.Join(root, "acks")} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create journal directory %s: %w", directory, err)
		}
	}
	epoch, err := loadOrCreateEpoch(root)
	if err != nil {
		return nil, err
	}
	j := &Journal{root: root, logPath: filepath.Join(root, "events.jsonl"), epoch: epoch}
	if err := j.load(); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *Journal) OperationID(sequence uint64) string {
	return fmt.Sprintf("%s:%020d", j.epoch, sequence)
}

func (j *Journal) Append(event Event, payloadSource string) (Event, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	event.Sequence = j.lastSequenceLocked() + 1
	if payloadSource != "" {
		payloadName := fmt.Sprintf("%020d.bin", event.Sequence)
		payloadPath := filepath.Join(j.root, "payloads", payloadName)
		if err := durableCopy(payloadSource, payloadPath); err != nil {
			return Event{}, err
		}
		event.Payload = payloadName
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return Event{}, fmt.Errorf("encode WAL event: %w", err)
	}
	log, err := os.OpenFile(j.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return Event{}, fmt.Errorf("open WAL: %w", err)
	}
	if _, err := log.Write(append(encoded, '\n')); err != nil {
		log.Close()
		return Event{}, fmt.Errorf("append WAL: %w", err)
	}
	if err := log.Sync(); err != nil {
		log.Close()
		return Event{}, fmt.Errorf("sync WAL: %w", err)
	}
	if err := log.Close(); err != nil {
		return Event{}, fmt.Errorf("close WAL: %w", err)
	}
	j.events = append(j.events, event)
	return event, nil
}

func (j *Journal) Pending() []Event {
	j.mu.Lock()
	defer j.mu.Unlock()
	index := sort.Search(len(j.events), func(index int) bool {
		return j.events[index].Sequence > j.ack
	})
	return append([]Event(nil), j.events[index:]...)
}

func (j *Journal) ReadPayload(event Event) ([]byte, error) {
	if event.Payload == "" {
		return nil, nil
	}
	return os.ReadFile(filepath.Join(j.root, "payloads", event.Payload))
}

func (j *Journal) Acknowledge(sequence uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if sequence <= j.ack {
		return nil
	}
	if sequence > j.lastSequenceLocked() {
		return fmt.Errorf("cannot acknowledge sequence %d beyond committed sequence %d", sequence, j.lastSequenceLocked())
	}
	ackPath := filepath.Join(j.root, "acks", fmt.Sprintf("%020d", sequence))
	ack, err := os.OpenFile(ackPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("create WAL acknowledgement: %w", err)
	}
	if err == nil {
		if syncErr := ack.Sync(); syncErr != nil {
			ack.Close()
			return fmt.Errorf("sync WAL acknowledgement: %w", syncErr)
		}
		if closeErr := ack.Close(); closeErr != nil {
			return fmt.Errorf("close WAL acknowledgement: %w", closeErr)
		}
	}
	j.ack = sequence
	return nil
}

func (j *Journal) SetLastError(err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err == nil {
		j.lastErr = ""
	} else {
		j.lastErr = err.Error()
	}
}

func (j *Journal) Status() Status {
	j.mu.Lock()
	defer j.mu.Unlock()
	last := j.lastSequenceLocked()
	return Status{
		AcknowledgedSequence:  j.ack,
		LastCommittedSequence: last,
		PendingEvents:         last - j.ack,
		LastError:             j.lastErr,
	}
}

func (j *Journal) Empty() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.events) == 0
}

func (j *Journal) load() error {
	log, err := os.Open(j.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return j.loadAcknowledgement()
		}
		return fmt.Errorf("open WAL for replay: %w", err)
	}
	defer log.Close()
	scanner := bufio.NewScanner(log)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var expected uint64 = 1
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("decode WAL sequence %d: %w", expected, err)
		}
		if event.Sequence != expected {
			return fmt.Errorf("WAL sequence discontinuity: expected %d, found %d", expected, event.Sequence)
		}
		j.events = append(j.events, event)
		expected++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan WAL: %w", err)
	}
	return j.loadAcknowledgement()
}

func (j *Journal) loadAcknowledgement() error {
	entries, err := os.ReadDir(filepath.Join(j.root, "acks"))
	if err != nil {
		return fmt.Errorf("read WAL acknowledgements: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		sequence, err := strconv.ParseUint(entry.Name(), 10, 64)
		if err == nil && sequence > j.ack {
			j.ack = sequence
		}
	}
	if j.ack > j.lastSequenceLocked() {
		return fmt.Errorf("WAL acknowledgement %d exceeds committed sequence %d", j.ack, j.lastSequenceLocked())
	}
	return nil
}

func (j *Journal) lastSequenceLocked() uint64 {
	if len(j.events) == 0 {
		return 0
	}
	return j.events[len(j.events)-1].Sequence
}

func durableCopy(sourcePath, destinationPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open payload source: %w", err)
	}
	defer source.Close()
	temporaryPath := destinationPath + ".tmp"
	destination, err := os.OpenFile(temporaryPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create payload: %w", err)
	}
	removeTemporary := true
	defer func() {
		destination.Close()
		if removeTemporary {
			os.Remove(temporaryPath)
		}
	}()
	if _, err := io.Copy(destination, source); err != nil {
		return fmt.Errorf("copy payload: %w", err)
	}
	if err := destination.Sync(); err != nil {
		return fmt.Errorf("sync payload: %w", err)
	}
	if err := destination.Close(); err != nil {
		return fmt.Errorf("close payload: %w", err)
	}
	if err := os.Rename(temporaryPath, destinationPath); err != nil {
		return fmt.Errorf("publish payload: %w", err)
	}
	removeTemporary = false
	return nil
}

func loadOrCreateEpoch(root string) (string, error) {
	path := filepath.Join(root, "epoch")
	if raw, err := os.ReadFile(path); err == nil {
		epoch := strings.TrimSpace(string(raw))
		if len(epoch) != 32 {
			return "", fmt.Errorf("journal epoch is malformed")
		}
		if _, err := hex.DecodeString(epoch); err != nil {
			return "", fmt.Errorf("journal epoch is malformed: %w", err)
		}
		return epoch, nil
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("read journal epoch: %w", err)
	}
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("mint journal epoch: %w", err)
	}
	epoch := hex.EncodeToString(bytes)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", fmt.Errorf("create journal epoch: %w", err)
	}
	if _, err := file.WriteString(epoch + "\n"); err != nil {
		file.Close()
		return "", fmt.Errorf("write journal epoch: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return "", fmt.Errorf("sync journal epoch: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close journal epoch: %w", err)
	}
	return epoch, nil
}

func ValidateRelativePath(name string) (string, error) {
	name = strings.Trim(strings.ReplaceAll(name, `\`, "/"), "/")
	if name == "" {
		return "", nil
	}
	for _, component := range strings.Split(name, "/") {
		if component == "" || component == "." || component == ".." || strings.ContainsRune(component, 0) {
			return "", fmt.Errorf("invalid VFS path %q", name)
		}
	}
	return name, nil
}
