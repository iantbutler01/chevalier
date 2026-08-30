package mount

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openbracket/chevalier/sandbox/winfsp-vfs/internal/gateway"
)

func TestSessionHydratesPublishesAndReplaysPendingWAL(t *testing.T) {
	type remoteState struct {
		sync.Mutex
		files    map[string][]byte
		dirs     map[string]bool
		online   bool
		revision uint64
	}
	remote := &remoteState{
		files:  map[string][]byte{"scope/seed.txt": []byte("seed")},
		dirs:   map[string]bool{"scope": true},
		online: true,
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		remote.Lock()
		defer remote.Unlock()
		if request.Header.Get("Authorization") != "Bearer token" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !remote.online {
			http.Error(response, "offline", http.StatusServiceUnavailable)
			return
		}
		switch request.URL.Path {
		case "/owner/tree":
			queryPath := request.URL.Query().Get("path")
			entries := []map[string]any{}
			if queryPath == "scope" {
				entries = append(entries, map[string]any{"name": "seed.txt", "kind": "file", "size_bytes": 4})
			}
			json.NewEncoder(response).Encode(entries)
		case "/owner/file/raw":
			body, ok := remote.files[request.URL.Query().Get("path")]
			if !ok {
				http.NotFound(response, request)
				return
			}
			response.Write(body)
		case "/owner/lease":
			response.Header().Set("x-chevalier-vfs-lease-mode", "implicit")
			json.NewEncoder(response).Encode(map[string]any{"resource_key": "rk", "owner_token": "owner"})
		case "/owner/write-many":
			var body struct {
				Writes []struct {
					Path string `json:"path"`
					Body string `json:"body_base64"`
				} `json:"writes"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			for _, write := range body.Writes {
				decoded, err := base64.StdEncoding.DecodeString(write.Body)
				if err != nil {
					t.Fatal(err)
				}
				remote.files[write.Path] = decoded
			}
			remote.revision++
			response.Header().Set("x-chevalier-vfs-namespace-revision", stringValue(remote.revision))
			json.NewEncoder(response).Encode(map[string]any{"entries": []any{}})
		case "/owner/namespace-many":
			var body struct {
				Mutations []map[string]any `json:"mutations"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			for _, mutation := range body.Mutations {
				switch mutation["kind"] {
				case "rename":
					from, to := mutation["from"].(string), mutation["to"].(string)
					remote.files[to] = remote.files[from]
					delete(remote.files, from)
				case "delete_file":
					delete(remote.files, mutation["path"].(string))
				}
			}
			remote.revision++
			response.Header().Set("x-chevalier-vfs-namespace-revision", stringValue(remote.revision))
			json.NewEncoder(response).Encode(map[string]any{"entries": []any{}})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client, err := gateway.New(server.URL+"/owner", "token", "scope")
	if err != nil {
		t.Fatal(err)
	}
	stateRoot := filepath.Join(t.TempDir(), "state")
	session, err := Open(context.Background(), stateRoot, client)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := os.ReadFile(filepath.Join(stateRoot, "tree", "seed.txt"))
	if err != nil || string(seed) != "seed" {
		t.Fatalf("hydrate seed: bytes=%q err=%v", seed, err)
	}
	session.StartPublisher()
	file, err := session.OpenFile("offline.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	remote.Lock()
	remote.online = false
	remote.Unlock()
	if _, err := file.Write([]byte("offline durable")); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatalf("local sync failed during gateway outage: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if session.Status().PendingEvents == 0 {
		t.Fatal("gateway outage did not retain a pending WAL event")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(context.Background(), stateRoot, client)
	if err != nil {
		t.Fatal(err)
	}
	local, err := os.ReadFile(filepath.Join(stateRoot, "tree", "offline.txt"))
	if err != nil || string(local) != "offline durable" {
		t.Fatalf("restart lost local state: bytes=%q err=%v", local, err)
	}
	remote.Lock()
	remote.online = true
	remote.Unlock()
	reopened.StartPublisher()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := reopened.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	remote.Lock()
	published := append([]byte(nil), remote.files["scope/offline.txt"]...)
	remote.Unlock()
	if string(published) != "offline durable" {
		t.Fatalf("unexpected published bytes %q", published)
	}
	status := reopened.Status()
	if status.PendingEvents != 0 || status.AcknowledgedSequence != status.LastCommittedSequence {
		t.Fatalf("WAL did not converge: %#v", status)
	}
}

func TestBackingHandlesDoNotBlockDelete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/owner/tree":
			json.NewEncoder(response).Encode([]map[string]any{})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client, err := gateway.New(server.URL+"/owner", "token", "scope")
	if err != nil {
		t.Fatal(err)
	}
	session, err := Open(context.Background(), filepath.Join(t.TempDir(), "state"), client)
	if err != nil {
		t.Fatal(err)
	}
	first, err := session.OpenFile("shared.txt", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := first.Write([]byte("shared")); err != nil {
		t.Fatal(err)
	}
	if err := first.Sync(); err != nil {
		t.Fatal(err)
	}
	second, err := session.OpenFile("shared.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := session.Remove("shared.txt"); err != nil {
		t.Fatalf("remove with open backing handles: %v", err)
	}
}

func TestOpenFileReportsMissingParentAsNotExist(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/owner/tree" {
			json.NewEncoder(response).Encode([]map[string]any{})
			return
		}
		http.NotFound(response, request)
	}))
	defer server.Close()
	client, err := gateway.New(server.URL+"/owner", "token", "scope")
	if err != nil {
		t.Fatal(err)
	}
	session, err := Open(context.Background(), filepath.Join(t.TempDir(), "state"), client)
	if err != nil {
		t.Fatal(err)
	}

	_, err = session.OpenFile("missing/child.txt", os.O_RDONLY, 0)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("OpenFile() error = %v, want os.ErrNotExist", err)
	}
	_, err = session.Stat("missing/child.txt")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat() error = %v, want os.ErrNotExist", err)
	}
}

func TestSessionUsesCanonicalBackingCaseForJournalPaths(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/owner/tree" {
			json.NewEncoder(response).Encode([]map[string]any{})
			return
		}
		http.NotFound(response, request)
	}))
	defer server.Close()
	client, err := gateway.New(server.URL+"/owner", "token", "scope")
	if err != nil {
		t.Fatal(err)
	}
	session, err := Open(context.Background(), filepath.Join(t.TempDir(), "state"), client)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Mkdir("repo", 0o700); err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile("REPO/config.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if file.relative != "repo/config.lock" {
		t.Fatalf("open path = %q, want repo/config.lock", file.relative)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.Rename("REPO/config.lock", "Repo/Config"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(session.treeRoot, "repo", "Config")); err != nil {
		t.Fatalf("case-preserving rename target: %v", err)
	}
}

func TestSessionRejectsCaseCollisionsDuringHydration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/owner/tree":
			json.NewEncoder(response).Encode([]map[string]any{
				{"name": "Readme.md", "kind": "file", "size_bytes": 3},
				{"name": "README.md", "kind": "file", "size_bytes": 3},
			})
		case "/owner/file/raw":
			response.Write([]byte("doc"))
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client, err := gateway.New(server.URL+"/owner", "token", "scope")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Open(context.Background(), filepath.Join(t.TempDir(), "state"), client)
	if err == nil || !strings.Contains(err.Error(), "case-colliding VFS entries") {
		t.Fatalf("Open() error = %v, want case collision", err)
	}
}

func stringValue(value uint64) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var buffer [20]byte
	index := len(buffer)
	for value > 0 {
		index--
		buffer[index] = digits[value%10]
		value /= 10
	}
	return string(buffer[index:])
}
