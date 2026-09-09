// Package main demonstrates a simple MCP server using NATS transport.
//
// This example shows how to:
// - Create an MCP server with a simple "greet" tool
// - Use the NATS transport to enable distributed MCP communication
// - Handle tool calls over NATS messaging
//
// To run this example:
//  1. Run this example: `go run main.go`
//     An embedded NATS server is started in-process and listens on
//     nats://127.0.0.1:4222 (override the port with NATS_PORT).
//     Set NATS_URL to use an external NATS server instead.
//  2. Test with NATS commands (see README.md for examples)
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ganawaj/mcp-transport-nats"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats-server/v2/server"
)

// HiArgs defines the input parameters for the greet tool
type HiArgs struct {
	Name string `json:"name" mcp:"the name to say hi to"`
}

// SayHi is a tool handler that responds with a personalized greeting.
// It demonstrates how to implement MCP tool functions that can be called
// remotely over NATS.
func SayHi(ctx context.Context, req *mcp.ServerRequest[*mcp.CallToolParamsFor[HiArgs]]) (*mcp.CallToolResultFor[struct{}], error) {
	name := req.Params.Arguments.Name

	// Log the tool invocation
	slog.Info("Greet tool invoked", "name", name)

	return &mcp.CallToolResultFor[struct{}]{
		Content: []mcp.Content{
			&mcp.TextContent{Text: "Hi " + name + "! 👋"},
		},
	}, nil
}

// startEmbeddedNATS runs a NATS server inside this process so the example
// works without any external dependencies. The server listens on 127.0.0.1
// and the port given by NATS_PORT (default 4222), so the NATS CLI in the
// README can still reach it.
//
// Returns the running server and its client URL. The caller is responsible
// for calling Shutdown and WaitForShutdown.
func startEmbeddedNATS(logger *slog.Logger) (*server.Server, string, error) {
	port := 4222
	if raw := os.Getenv("NATS_PORT"); raw != "" {
		p, err := strconv.Atoi(raw)
		if err != nil || p < 0 || p > 65535 {
			return nil, "", fmt.Errorf("invalid NATS_PORT %q", raw)
		}
		port = p
	}

	ns, err := server.NewServer(&server.Options{
		ServerName: "mcp-greeter-embedded",
		Host:       "127.0.0.1",
		Port:       port,
		NoSigs:     true, // main handles signals itself
	})
	if err != nil {
		return nil, "", fmt.Errorf("create embedded NATS server: %w", err)
	}

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		return nil, "", fmt.Errorf("embedded NATS server did not become ready on port %d", port)
	}

	logger.Info("Embedded NATS server started", "url", ns.ClientURL())
	return ns, ns.ClientURL(), nil
}

func main() {
	// Create a structured logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Use an external NATS server if NATS_URL is set, otherwise embed one.
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		ns, url, err := startEmbeddedNATS(logger.With("component", "nats-server"))
		if err != nil {
			log.Fatal("Failed to start embedded NATS server: ", err)
		}
		defer func() {
			ns.Shutdown()
			ns.WaitForShutdown()
		}()
		natsURL = url
	}

	// Create MCP server with server info
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "greeter",
		Version: "1.0.0",
	}, nil)

	// Register the greet tool
	mcp.AddTool(server, &mcp.Tool{
		Name:        "greet",
		Description: "Greet someone with a friendly message",
	}, SayHi)

	// Create NATS transport
	transport := &natstransport.Transport{
		Subject: "mcp.greeter",
		NatsURL: natsURL,
		Logger:  logger.With("component", "transport"),
	}

	logger.Info("Starting MCP server with NATS transport",
		"subject", transport.Subject,
		"nats_url", transport.NatsURL)

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, stopping server...")
		cancel()
	}()

	// Run the server
	if err := server.Run(ctx, transport); err != nil {
		if ctx.Err() != nil {
			logger.Info("Server stopped gracefully")
		} else {
			log.Fatal("Failed to run server:", err)
		}
	}
}
