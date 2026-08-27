package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

const namespaceRevisionHeader = "x-chevalier-vfs-namespace-revision"

type Client struct {
	endpoint   string
	token      string
	scope      string
	httpClient *http.Client
}

type DirEntry struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	SizeBytes  uint64 `json:"size_bytes"`
	LinkTarget string `json:"link_target"`
}

type Event struct {
	Sequence    uint64
	OperationID string
	Kind        string
	Path        string
	From        string
	To          string
	Mode        uint32
	Payload     []byte
}

func New(endpoint, token, scope string) (*Client, error) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("gateway endpoint is required")
	}
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return nil, fmt.Errorf("parse gateway endpoint: %w", err)
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("gateway bearer token is required")
	}
	scope, err := validateScope(scope)
	if err != nil {
		return nil, err
	}
	return &Client{
		endpoint: endpoint,
		token:    strings.TrimSpace(token),
		scope:    scope,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func validateScope(scope string) (string, error) {
	scope = strings.Trim(strings.ReplaceAll(scope, `\`, "/"), "/")
	if scope == "" {
		return "", fmt.Errorf("gateway scope is required")
	}
	for _, component := range strings.Split(scope, "/") {
		if component == "" || component == "." || component == ".." || strings.ContainsRune(component, 0) {
			return "", fmt.Errorf("invalid gateway scope %q", scope)
		}
	}
	return scope, nil
}

func (c *Client) ListDirectory(ctx context.Context, relative string) ([]DirEntry, error) {
	query := url.Values{"path": []string{c.scoped(relative)}, "max_hash_bytes": []string{"1048576"}}
	response, err := c.request(ctx, http.MethodGet, "/tree?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, responseError(response)
	}
	var entries []DirEntry
	if err := json.NewDecoder(response.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("decode directory listing: %w", err)
	}
	return entries, nil
}

func (c *Client) ReadFile(ctx context.Context, relative string) ([]byte, error) {
	query := url.Values{"path": []string{c.scoped(relative)}}
	response, err := c.request(ctx, http.MethodGet, "/file/raw?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, responseError(response)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read gateway file response: %w", err)
	}
	return body, nil
}

func (c *Client) Publish(ctx context.Context, event Event) (uint64, error) {
	lease, implicit, err := c.acquireLease(ctx, event)
	if err != nil {
		return 0, err
	}
	if !implicit {
		defer c.releaseLease(context.WithoutCancel(ctx), lease)
	}

	var route string
	var body any
	operation := "vfs_namespace_batch"
	if event.Kind == "replace_file" {
		route = "/write-many"
		operation = "vfs_write_many"
		body = map[string]any{"writes": []any{map[string]any{
			"path":        c.scoped(event.Path),
			"body_base64": base64.StdEncoding.EncodeToString(event.Payload),
			"mode":        event.Mode,
		}}}
	} else {
		if strings.TrimSpace(event.OperationID) == "" {
			return 0, fmt.Errorf("namespace mutation requires a stable operation ID")
		}
		route = "/namespace-many"
		mutation, err := c.namespaceMutation(event)
		if err != nil {
			return 0, err
		}
		body = map[string]any{
			"operation_ids": []string{event.OperationID},
			"mutations":     []any{mutation},
		}
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("encode gateway mutation: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+route, bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("build gateway mutation: %w", err)
	}
	c.authorize(request)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-chevalier-vfs-component", "vm_runtime")
	request.Header.Set("x-chevalier-vfs-surface-kind", "vm_workspace_vfs")
	request.Header.Set("x-chevalier-vfs-operation", operation)
	request.Header.Set("x-chevalier-vfs-resource-key", lease.ResourceKey)
	request.Header.Set("x-chevalier-vfs-lock-owner-token", lease.OwnerToken)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return 0, fmt.Errorf("publish gateway mutation: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return 0, responseError(response)
	}
	revision, err := strconv.ParseUint(response.Header.Get(namespaceRevisionHeader), 10, 64)
	if err != nil || revision == 0 {
		return 0, fmt.Errorf("gateway mutation omitted a valid %s", namespaceRevisionHeader)
	}
	return revision, nil
}

type leaseGrant struct {
	ResourceKey string `json:"resource_key"`
	OwnerToken  string `json:"owner_token"`
}

func (c *Client) acquireLease(ctx context.Context, event Event) (leaseGrant, bool, error) {
	body, err := json.Marshal(map[string]any{
		"path":           c.scoped(event.Path),
		"mutation_count": 1,
		"component":      "vm_runtime",
		"reason":         "publish Windows guest WinFsp WAL event",
	})
	if err != nil {
		return leaseGrant{}, false, err
	}
	response, err := c.request(ctx, http.MethodPost, "/lease", bytes.NewReader(body))
	if err != nil {
		return leaseGrant{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return leaseGrant{}, false, responseError(response)
	}
	var grant leaseGrant
	if err := json.NewDecoder(response.Body).Decode(&grant); err != nil {
		return leaseGrant{}, false, fmt.Errorf("decode gateway lease: %w", err)
	}
	if grant.ResourceKey == "" || grant.OwnerToken == "" {
		return leaseGrant{}, false, fmt.Errorf("gateway returned an incomplete lease")
	}
	return grant, response.Header.Get("x-chevalier-vfs-lease-mode") == "implicit", nil
}

func (c *Client) releaseLease(ctx context.Context, grant leaseGrant) {
	body, err := json.Marshal(map[string]any{
		"resource_key": grant.ResourceKey,
		"owner_token":  grant.OwnerToken,
	})
	if err != nil {
		return
	}
	response, err := c.request(ctx, http.MethodDelete, "/lease", bytes.NewReader(body))
	if err == nil {
		response.Body.Close()
	}
}

func (c *Client) namespaceMutation(event Event) (map[string]any, error) {
	switch event.Kind {
	case "create_directory":
		return map[string]any{"kind": event.Kind, "path": c.scoped(event.Path), "mode": event.Mode}, nil
	case "delete_file", "remove_directory":
		return map[string]any{"kind": event.Kind, "path": c.scoped(event.Path)}, nil
	case "rename":
		return map[string]any{"kind": event.Kind, "from": c.scoped(event.From), "to": c.scoped(event.To)}, nil
	default:
		return nil, fmt.Errorf("unsupported WAL event kind %q", event.Kind)
	}
}

func (c *Client) scoped(relative string) string {
	relative = strings.Trim(strings.ReplaceAll(relative, `\`, "/"), "/")
	if c.scope == "" {
		return relative
	}
	if relative == "" {
		return c.scope
	}
	return path.Join(c.scope, relative)
}

func (c *Client) request(ctx context.Context, method, suffix string, body io.Reader) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, c.endpoint+suffix, body)
	if err != nil {
		return nil, fmt.Errorf("build gateway request: %w", err)
	}
	c.authorize(request)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send gateway request: %w", err)
	}
	return response, nil
}

func (c *Client) authorize(request *http.Request) {
	request.Header.Set("Authorization", "Bearer "+c.token)
}

func responseError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	return fmt.Errorf("gateway returned %s: %s", response.Status, strings.TrimSpace(string(body)))
}
