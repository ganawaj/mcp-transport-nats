// Package main demonstrates a simple MCP server using NATS transport.
//
// This example shows how to:
// - Create an MCP server with a simple "greet" tool
// - Use the NATS transport to enable distributed MCP communication
// - Handle tool calls over NATS messaging
//
// To run this example:
// 1. Start a NATS server: `nats-server`
// 2. Run this example: `go run main.go`
// 3. Test with NATS commands (see README.md for examples)
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ganawaj/mcp-transport-nats"
	"github.com/modelcontextprotocol/go-sdk/mcp"
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

func main() {
	// Create a structured logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

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
		NatsURL: "nats://localhost:4222",
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
