package journal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJournalReplaysPayloadAndDurableAcknowledgement(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.WriteFile(source, []byte("durable payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	j, err := Open(filepath.Join(root, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := j.Append(Event{Kind: "replace_file", Path: "repo/file.txt", Mode: 0o644}, source)
	if err != nil {
		t.Fatal(err)
	}
	second, err := j.Append(Event{Kind: "rename", From: "repo/file.txt", To: "repo/moved.txt"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 2 {
		t.Fatalf("unexpected sequences: %d, %d", first.Sequence, second.Sequence)
	}
	firstOperationID := j.OperationID(first.Sequence)
	if err := j.Acknowledge(1); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(filepath.Join(root, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	pending := reopened.Pending()
	if len(pending) != 1 || pending[0].Sequence != 2 {
		t.Fatalf("unexpected pending WAL: %#v", pending)
	}
	payload, err := reopened.ReadPayload(first)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "durable payload" {
		t.Fatalf("unexpected payload %q", payload)
	}
	status := reopened.Status()
	if status.AcknowledgedSequence != 1 || status.LastCommittedSequence != 2 || status.PendingEvents != 1 {
		t.Fatalf("unexpected status: %#v", status)
	}
	if reopened.OperationID(first.Sequence) != firstOperationID {
		t.Fatal("journal operation ID changed across restart")
	}
}

func TestValidateRelativePathRejectsEscapes(t *testing.T) {
	for _, invalid := range []string{"../escape", `folder\..\escape`, "./file", "folder//file"} {
		if _, err := ValidateRelativePath(invalid); err == nil {
			t.Fatalf("accepted invalid path %q", invalid)
		}
	}
	got, err := ValidateRelativePath(`repo\src\main.go`)
	if err != nil {
		t.Fatal(err)
	}
	if got != "repo/src/main.go" {
		t.Fatalf("unexpected normalized path %q", got)
	}
}
