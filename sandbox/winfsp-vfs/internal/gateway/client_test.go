package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientScopesAuthenticatesAndPublishesStableOperation(t *testing.T) {
	var sawWrite bool
	var sawRename bool
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer secret" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch request.URL.Path {
		case "/owner/lease":
			response.Header().Set("x-chevalier-vfs-lease-mode", "implicit")
			json.NewEncoder(response).Encode(map[string]any{"resource_key": "rk", "owner_token": "owner"})
		case "/owner/tree":
			if request.URL.Query().Get("path") != "scope/repo" {
				t.Fatalf("unexpected scoped tree path %q", request.URL.Query().Get("path"))
			}
			json.NewEncoder(response).Encode([]DirEntry{{Name: "a.txt", Kind: "file", SizeBytes: 1}})
		case "/owner/file/raw":
			response.Write([]byte("a"))
		case "/owner/write-many":
			if request.Header.Get("x-chevalier-vfs-operation") != "vfs_write_many" {
				t.Fatalf("missing write operation header")
			}
			var body struct {
				Writes []struct {
					Path string `json:"path"`
					Body string `json:"body_base64"`
				} `json:"writes"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if len(body.Writes) != 1 || body.Writes[0].Path != "scope/repo/a.txt" || body.Writes[0].Body != "YQ==" {
				t.Fatalf("unexpected write body: %#v", body)
			}
			sawWrite = true
			response.Header().Set(namespaceRevisionHeader, "7")
			json.NewEncoder(response).Encode(map[string]any{"entries": []any{}})
		case "/owner/namespace-many":
			var body struct {
				OperationIDs []string         `json:"operation_ids"`
				Mutations    []map[string]any `json:"mutations"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if len(body.OperationIDs) != 1 || body.OperationIDs[0] != "epoch:00000000000000000008" {
				t.Fatalf("unstable operation id: %#v", body.OperationIDs)
			}
			if body.Mutations[0]["from"] != "scope/repo/a.txt" || body.Mutations[0]["to"] != "scope/repo/b.txt" {
				t.Fatalf("unexpected rename: %#v", body.Mutations[0])
			}
			sawRename = true
			response.Header().Set(namespaceRevisionHeader, "8")
			json.NewEncoder(response).Encode(map[string]any{"entries": []any{}})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client, err := New(server.URL+"/owner", "secret", "scope")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := client.ListDirectory(context.Background(), "repo")
	if err != nil || len(entries) != 1 {
		t.Fatalf("list directory: entries=%#v err=%v", entries, err)
	}
	if revision, err := client.Publish(context.Background(), Event{Sequence: 7, Kind: "replace_file", Path: "repo/a.txt", Payload: []byte("a"), Mode: 0o644}); err != nil || revision != 7 {
		t.Fatalf("publish write: revision=%d err=%v", revision, err)
	}
	if revision, err := client.Publish(context.Background(), Event{Sequence: 8, OperationID: "epoch:00000000000000000008", Kind: "rename", Path: "repo/b.txt", From: "repo/a.txt", To: "repo/b.txt"}); err != nil || revision != 8 {
		t.Fatalf("publish rename: revision=%d err=%v", revision, err)
	}
	if !sawWrite || !sawRename {
		t.Fatalf("missing mutation: write=%t rename=%t", sawWrite, sawRename)
	}
}

func TestClientRejectsUnscopedOrEscapingAssignments(t *testing.T) {
	for _, scope := range []string{"", ".", "..", "workspace/../other", "workspace//other"} {
		if _, err := New("http://127.0.0.1/owner", "secret", scope); err == nil {
			t.Fatalf("New() accepted invalid scope %q", scope)
		}
	}
}
