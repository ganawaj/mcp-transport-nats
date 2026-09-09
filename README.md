# MCP NATS Transport

A [NATS](https://nats.io/) transport implementation for the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). This enables **distributed MCP communication** by routing JSON-RPC messages over NATS messaging.

[![Go Reference](https://pkg.go.dev/badge/github.com/ganawaj/mcp-transport-nats.svg)](https://pkg.go.dev/github.com/ganawaj/mcp-transport-nats)
[![Go Report Card](https://goreportcard.com/badge/github.com/ganawaj/mcp-transport-nats)](https://goreportcard.com/report/github.com/ganawaj/mcp-transport-nats)

## What This Enables

- **Distributed MCP**: Run MCP servers and clients across network boundaries
- **Service Discovery**: Leverage NATs subjects for discovery
- **Load Balancing**: Use NATS queue groups for automatic load distribution
- **Fault Tolerance**: Built-in reconnection and error recovery
- **Scalability**: Handle thousands of concurrent MCP connections

## Installation

```bash
go get github.com/ganawaj/mcp-transport-nats
```

## Quick Start

### 1. Start NATS Server

```bash
# Start NATS server
docker run -d --rm -p 4222:4222 nats:latest -DV -js
```

### 2. Create an MCP Server

```go
package main

import (
    "context"
    "log"

    mcpNats "github.com/ganawaj/mcp-transport-nats"
    "github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
    // Create MCP server
    server := mcp.NewServer(&mcp.Implementation{
        Name: "my-service",
        Version: "1.0.0",
    }, nil)

    // Add tools (see examples for details)
    mcp.AddTool(server, &mcp.Tool{Name: "greet"}, greetHandler)

    // Create NATS transport
    transport := &mcpNats.Transport{
        Subject: "mcp.my-service",
        NatsURL: "nats://localhost:4222",
    }

    // Run server
    err := server.Run(context.Background(), transport)
    if err != nil {
        log.Fatal(err)
    }
}
```

### 3. Test with NATS CLI

```bash
# Install NATS CLI
go install github.com/nats-io/natscli/nats@latest

# Initialize MCP session
nats request mcp.my-service '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{}},"id":1}'

# Send initialized notification
nats pub mcp.my-service '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'

# List available tools
nats request mcp.my-service '{"jsonrpc":"2.0","method":"tools/list","params":{},"id":2}'

# Call a tool
nats request mcp.my-service '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"greet","arguments":{"name":"World"}},"id":3}'
```

## Configuration

### Transport Options

```go
transport := &mcp_nats.Transport{
    // Required: NATS subject to listen on
    // Must be a valid NATs subject: https://docs.nats.io/nats-concepts/subjects
    Subject: "mcp.my-service",

    // Optional: NATS server URL (default: nats://localhost:4222)
    NatsURL: "nats://localhost:4222",

    // Optional: Queue group for load balancing
    // See: https://docs.nats.io/nats-concepts/core-nats/queue
    Queue: "my-service-workers",

    // Optional: Custom logger
    Logger: slog.Default().With("service", "my-service"),

    // Optional: Connection timeouts
    ConnectTimeout: 30 * time.Second,
    ReconnectWait:  5 * time.Second,
    MaxReconnects:  -1, // Unlimited
}
```

### Subject Naming Conventions

The MCP listens on a typical [Nats Subject](https://docs.nats.io/nats-concepts/subjects). This include both wildcards, '*' and '>'.

```
mcp.<service>.<instance>     # Single service instance
mcp.<domain>.<service>       # Service within a domain
mcp.agents.<agent-id>        # Individual agents (A2A discovery)
mcp.tools.<tool-name>        # Tool-specific services
```

Examples:
- `mcp.agents.sales-bot-1` - Sales agent instance
- `mcp.finance.calculator` - Finance calculation service

## Examples

### Simple Server with Tool

See [`examples/simple-server/`](examples/simple-server/) for a complete working example.

### Testing the Example

```bash
# Terminal 1: Run the example
# An embedded NATS server starts in-process on nats://127.0.0.1:4222.
# Set NATS_PORT to change the port, or NATS_URL to use an external server:
#   NATS_URL=nats://localhost:4222 go run main.go
cd examples/simple-server
go run main.go

# Terminal 2: Test the server
# Initialize session
nats request mcp.greeter '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{}},"id":1}'

# Send initialized notification
nats pub mcp.greeter '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'

# List tools
nats request mcp.greeter '{"jsonrpc":"2.0","method":"tools/list","params":{},"id":2}'

# Call the greet tool
nats request mcp.greeter '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"greet","arguments":{"name":"Alice"}},"id":3}'

# Expected response:
# {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"Hi Alice! 👋"}],"structuredContent":{}}}
```

## Architecture

### How It Works

```
┌─────────────┐    NATS     ┌─────────────┐
│ MCP Client  │◄──────────► │ MCP Server  │
│             │  JSON-RPC   │             │
└─────────────┘   over      └─────────────┘
                  NATS
```

1. **MCP Server** uses NATS transport and subscribes to a subject
2. **MCP Client** sends JSON-RPC requests to the subject
3. **NATS** routes messages and handles replies automatically
4. **Transport** handles encoding/decoding and session management

### Message Flow

```
Client                NATS                Server
  │                    │                    │
  │─── JSON-RPC ──────►│                    │
  │    Request         │──── Forward ─────►│
  │                    │                    │───┐
  │                    │                    │   │ Process
  │                    │                    │   │ Request
  │                    │                    │◄──┘
  │                    │◄─── Response ─────│
  │◄── JSON-RPC ──────│                    │
      Response
```

### Load Balancing

Use NATS queue groups for automatic load balancing:

```go
// Multiple instances with same queue group
transport := &mcp_nats.Transport{
    Subject: "mcp.my-service",
    Queue:   "my-service-workers", // Same queue group
}
```

NATS automatically distributes requests across instances in the same queue group. Queue groups guarantees that the message is only delivered once for all subscribers to the queue.

## Monitoring and Observability

The transport provides structured logging for observability:

```go
import "log/slog"

logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

transport := &mcp_nats.Transport{
    Subject: "mcp.my-service",
    Logger:  logger,
}
```

### Log Events

- Connection establishment and health
- Message routing and session management
- Error conditions and recovery
- Performance metrics

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## Related Projects

- [MCP Go SDK](https://github.com/modelcontextprotocol/go-sdk) - Official MCP Go implementation
- [NATS Server](https://github.com/nats-io/nats-server) - High-performance messaging system

---
# mcp-transport-nats
