package natstransport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startNATS runs an embedded NATS server on a random loopback port and
// returns its client URL. The server is shut down when the test ends.
func startNATS(t *testing.T) string {
	t.Helper()

	ns, err := server.NewServer(&server.Options{
		Host:   "127.0.0.1",
		Port:   -1, // pick a free port
		NoSigs: true,
		NoLog:  true,
	})
	if err != nil {
		t.Fatalf("create embedded NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS server did not become ready")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return ns.ClientURL()
}

// rpc sends a JSON-RPC request over NATS and decodes the "result" field of
// the reply into out.
func rpc(t *testing.T, nc *nats.Conn, subject, method string, id int, params any, out any) {
	t.Helper()

	req, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		t.Fatalf("marshal %s request: %v", method, err)
	}

	msg, err := nc.Request(subject, req, 3*time.Second)
	if err != nil {
		t.Fatalf("%s request: %v", method, err)
	}

	var resp struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("decode %s response %q: %v", method, msg.Data, err)
	}
	if resp.Error != nil {
		t.Fatalf("%s returned JSON-RPC error: %s", method, resp.Error)
	}
	if resp.ID != id {
		t.Fatalf("%s response id = %d, want %d", method, resp.ID, id)
	}
	if out != nil {
		if err := json.Unmarshal(resp.Result, out); err != nil {
			t.Fatalf("decode %s result %q: %v", method, resp.Result, err)
		}
	}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type greetArgs struct {
	Name string `json:"name" jsonschema:"who to greet"`
}

func TestConnectRejectsUnreachableServer(t *testing.T) {
	tr := &Transport{
		Subject: "mcp.test",
		NatsURL: "nats://127.0.0.1:1", // nothing listens on port 1
		Logger:  quietLogger(),
	}
	if _, err := tr.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded against an unreachable server")
	}
}

func TestConnectRejectsEmptySubject(t *testing.T) {
	tr := &Transport{
		Subject: "",
		NatsURL: startNATS(t),
		Logger:  quietLogger(),
	}
	if _, err := tr.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded with an empty subject")
	}
}

// TestServerRoundTrip runs a real mcp.Server over the transport and drives
// the full initialize / tools/list / tools/call sequence from a plain NATS
// client, the same way the README examples do.
func TestServerRoundTrip(t *testing.T) {
	url := startNATS(t)
	const subject = "mcp.greeter.test"

	srv := mcp.NewServer(&mcp.Implementation{Name: "greeter", Version: "test"}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "greet", Description: "say hi"},
		func(_ context.Context, _ *mcp.CallToolRequest, args greetArgs) (*mcp.CallToolResult, any, error) {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Hi " + args.Name}},
			}, nil, nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- srv.Run(ctx, &Transport{Subject: subject, NatsURL: url, Logger: quietLogger()})
	}()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect test client: %v", err)
	}
	defer nc.Close()

	// The server subscribes asynchronously inside Run; wait until a
	// request gets a responder rather than sleeping a fixed amount.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := nc.Request(subject, []byte(`{"jsonrpc":"2.0","id":0,"method":"ping"}`), 200*time.Millisecond)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never started responding on %s: %v", subject, err)
		}
	}

	var initResult struct {
		ProtocolVersion string `json:"protocolVersion"`
		ServerInfo      struct {
			Name string `json:"name"`
		} `json:"serverInfo"`
	}
	rpc(t, nc, subject, "initialize", 1, map[string]any{
		"protocolVersion": "2025-06-18",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test", "version": "0"},
	}, &initResult)
	if initResult.ServerInfo.Name != "greeter" {
		t.Errorf("serverInfo.name = %q, want greeter", initResult.ServerInfo.Name)
	}
	if initResult.ProtocolVersion == "" {
		t.Error("initialize result has no protocolVersion")
	}

	if err := nc.Publish(subject, []byte(`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`)); err != nil {
		t.Fatalf("publish initialized notification: %v", err)
	}

	var listResult struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	rpc(t, nc, subject, "tools/list", 2, map[string]any{}, &listResult)
	if len(listResult.Tools) != 1 || listResult.Tools[0].Name != "greet" {
		t.Errorf("tools/list = %+v, want exactly the greet tool", listResult.Tools)
	}

	var callResult struct {
		IsError bool `json:"isError"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	rpc(t, nc, subject, "tools/call", 3, map[string]any{
		"name":      "greet",
		"arguments": map[string]any{"name": "Alice"},
	}, &callResult)
	if callResult.IsError {
		t.Errorf("tools/call reported isError")
	}
	if len(callResult.Content) != 1 || callResult.Content[0].Text != "Hi Alice" {
		t.Errorf("tools/call content = %+v, want a single 'Hi Alice' text item", callResult.Content)
	}

	// Cancelling the run context must actually stop the server. This
	// guards against the Read-blocks-forever-after-Close regression.
	cancel()
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server.Run did not return after context cancellation")
	}
}

func TestCloseUnblocksRead(t *testing.T) {
	tr := &Transport{Subject: "mcp.close.test", NatsURL: startNATS(t), Logger: quietLogger()}
	conn, err := tr.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	readErr := make(chan error, 1)
	go func() {
		_, err := conn.Read(context.Background())
		readErr <- err
	}()

	// Give Read a moment to park on the incoming channel before closing.
	time.Sleep(50 * time.Millisecond)

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-readErr:
		if !errors.Is(err, io.EOF) {
			t.Errorf("Read after Close returned %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not return after Close")
	}

	// Close must be idempotent.
	if err := conn.Close(); err != nil {
		t.Errorf("second Close returned %v, want nil", err)
	}
}

func TestWriteWithoutReplySubject(t *testing.T) {
	tr := &Transport{Subject: "mcp.write.test", NatsURL: startNATS(t), Logger: quietLogger()}
	conn, err := tr.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	// No request has been read yet, so there is nowhere to send a reply.
	err = conn.Write(context.Background(), &jsonrpc.Request{Method: "ping"})
	if err == nil || !strings.Contains(err.Error(), "no reply subject") {
		t.Fatalf("Write before any Read returned %v, want 'no reply subject' error", err)
	}
}
