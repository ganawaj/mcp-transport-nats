// Package mcp_nats provides a NATS transport implementation for the Model Context Protocol (MCP).
//
// This package enables distributed MCP communication by implementing the MCP Transport interface
// over NATS messaging. It allows MCP servers and clients to communicate across network boundaries
// using NATS as the message broker.
//
// Example usage:
//
//	transport := &mcp_nats.Transport{
//		Subject: "mcp.service.registration",
//		NatsURL: "nats://localhost:4222",
//	}
//
//	server := mcp.NewServer(&mcp.Implementation{Name: "my-service"}, nil)
//	err := server.Run(ctx, transport)
package natstransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats.go"
)

// Transport implements the MCP Transport interface using NATS as the underlying messaging system.
// It enables distributed MCP communication by routing JSON-RPC messages over NATS subjects.
type Transport struct {
	// Subject is the NATS subject on which the MCP server will listen for incoming requests.
	// Example: "mcp.service.registration" or "mcp.agent.sales"
	Subject string

	// NatsURL is the NATS server connection URL.
	// If empty, defaults to nats.DefaultURL ("nats://localhost:4222")
	NatsURL string

	// Queue is an optional NATS queue group name for load balancing.
	// When specified, multiple instances of the same service can share the workload,
	// with only one instance processing each message.
	Queue string

	// Logger is an optional structured logger for transport operations.
	// If nil, a default logger will be created.
	Logger *slog.Logger

	// ConnectTimeout specifies the timeout for NATS connection establishment.
	// If zero, defaults to 30 seconds.
	ConnectTimeout time.Duration

	// ReconnectWait specifies the time to wait between reconnection attempts.
	// If zero, defaults to 5 seconds.
	ReconnectWait time.Duration

	// MaxReconnects specifies the maximum number of reconnection attempts.
	// If zero, defaults to unlimited (-1).
	MaxReconnects int

	// connection holds the active NATS connection, set during Connect()
	connection *nats.Conn
}

// Connection represents an active MCP connection over NATS.
// It implements the mcp.Connection interface for bidirectional JSON-RPC communication.
type Connection struct {
	sessionID    string
	nc           *nats.Conn
	incoming     chan *nats.Msg
	subscription *nats.Subscription
	currentMsg   *nats.Msg
	logger       *slog.Logger
}

// Connect establishes a NATS connection and returns an MCP Connection.
// This method implements the mcp.Transport interface.
//
// The connection process:
// 1. Establishes NATS connection with configured options
// 2. Subscribes to the specified subject (optionally with queue group)
// 3. Sets up message routing for JSON-RPC communication
// 4. Returns a Connection ready for MCP protocol messages
func (t *Transport) Connect(ctx context.Context) (mcp.Connection, error) {
	logger := t.getLogger()
	logger.Info("Establishing NATS transport connection",
		"subject", t.Subject,
		"nats_url", t.getNatsURL(),
		"queue", t.Queue)

	// Prevent duplicate connections
	if t.connection != nil && t.connection.Status() == nats.CONNECTED {
		return nil, fmt.Errorf("transport already connected to NATS server at %s", t.getNatsURL())
	}

	// Connect to NATS with configured options
	nc, err := nats.Connect(t.getNatsURL(), t.getNatsOptions(logger)...)
	if err != nil {
		logger.Error("Failed to connect to NATS server",
			"nats_url", t.getNatsURL(),
			"error", err)
		return nil, fmt.Errorf("failed to connect to NATS server at %s: %w", t.getNatsURL(), err)
	}

	t.connection = nc
	logger.Info("Successfully connected to NATS server",
		"server_url", nc.ConnectedUrl(),
		"server_id", nc.ConnectedServerId())

	// Create buffered channel for incoming messages
	incoming := make(chan *nats.Msg, 100)

	// Subscribe to the subject with optional queue group
	var sub *nats.Subscription
	if t.Queue != "" {
		sub, err = nc.ChanQueueSubscribe(t.Subject, t.Queue, incoming)
		logger.Info("Subscribed to NATS subject with queue group",
			"subject", t.Subject,
			"queue", t.Queue)
	} else {
		sub, err = nc.ChanSubscribe(t.Subject, incoming)
		logger.Info("Subscribed to NATS subject",
			"subject", t.Subject)
	}

	if err != nil {
		nc.Close()
		logger.Error("Failed to subscribe to NATS subject",
			"subject", t.Subject,
			"queue", t.Queue,
			"error", err)
		return nil, fmt.Errorf("failed to subscribe to subject %s: %w", t.Subject, err)
	}

	logger.Info("NATS transport connection established successfully",
		"subject", t.Subject,
		"connection_status", nc.Status())

	return &Connection{
		nc:           nc,
		incoming:     incoming,
		subscription: sub,
		logger:       logger,
	}, nil
}

// Read reads the next JSON-RPC message from the NATS connection.
// This method implements the mcp.Connection interface.
//
// The read process:
// 1. Waits for a message on the NATS channel (respecting context cancellation)
// 2. Stores the message for reply routing in Write()
// 3. Decodes the message data as JSON-RPC using the MCP SDK
// 4. Returns the parsed message for processing by the MCP server
func (c *Connection) Read(ctx context.Context) (jsonrpc.Message, error) {
	if c.nc == nil {
		return nil, errors.New("not connected to NATS server")
	}

	select {
	case <-ctx.Done():
		c.logger.Debug("Read operation cancelled", "reason", ctx.Err())
		return nil, ctx.Err()

	case msg := <-c.incoming:
		if msg == nil {
			c.logger.Warn("Received nil message from NATS channel")
			return nil, io.EOF
		}

		c.logger.Debug("Received NATS message",
			"subject", msg.Subject,
			"reply", msg.Reply,
			"size", len(msg.Data))

		// Store the current message for reply routing
		c.currentMsg = msg

		// Update session ID from reply subject if available
		if msg.Reply != "" {
			c.sessionID = msg.Reply
			c.logger.Debug("Updated session ID from reply subject", "session_id", c.sessionID)
		}

		// Decode JSON-RPC message using MCP SDK
		jsonMsg, err := jsonrpc.DecodeMessage(msg.Data)
		if err != nil {
			c.logger.Error("Failed to decode JSON-RPC message",
				"error", err,
				"data", string(msg.Data))
			return nil, fmt.Errorf("failed to decode JSON-RPC message: %w", err)
		}

		c.logger.Debug("Successfully decoded JSON-RPC message",
			"message_type", fmt.Sprintf("%T", jsonMsg))

		return jsonMsg, nil
	}
}

// Write sends a JSON-RPC message response over the NATS connection.
// This method implements the mcp.Connection interface.
//
// The write process:
// 1. Encodes the JSON-RPC message using the MCP SDK
// 2. Determines the reply subject from the current request
// 3. Publishes the response to the appropriate NATS subject
func (c *Connection) Write(ctx context.Context, msg jsonrpc.Message) error {
	if c.nc == nil {
		return errors.New("not connected to NATS server")
	}

	// Determine the reply subject
	var replySubject string
	if c.currentMsg != nil && c.currentMsg.Reply != "" {
		replySubject = c.currentMsg.Reply
	} else if c.sessionID != "" {
		replySubject = c.sessionID
	} else {
		c.logger.Error("No reply subject available for response")
		return errors.New("no reply subject available")
	}

	// Encode JSON-RPC message using MCP SDK
	msgBytes, err := jsonrpc.EncodeMessage(msg)
	if err != nil {
		c.logger.Error("Failed to encode JSON-RPC message",
			"error", err,
			"message_type", fmt.Sprintf("%T", msg))
		return fmt.Errorf("failed to encode JSON-RPC message: %w", err)
	}

	c.logger.Debug("Sending JSON-RPC response",
		"reply_subject", replySubject,
		"size", len(msgBytes),
		"message_type", fmt.Sprintf("%T", msg))

	// Publish response to NATS
	if err := c.nc.Publish(replySubject, msgBytes); err != nil {
		c.logger.Error("Failed to publish response to NATS",
			"reply_subject", replySubject,
			"error", err)
		return fmt.Errorf("failed to publish response: %w", err)
	}

	c.logger.Debug("Successfully sent JSON-RPC response",
		"reply_subject", replySubject)

	return nil
}

// Close closes the NATS connection and cleans up resources.
// This method implements the mcp.Connection interface.
func (c *Connection) Close() error {
	if c.nc == nil {
		return nil
	}

	c.logger.Info("Closing NATS connection")

	// Unsubscribe if subscription exists
	if c.subscription != nil {
		if err := c.subscription.Unsubscribe(); err != nil {
			c.logger.Warn("Failed to unsubscribe from NATS subject", "error", err)
		}
	}

	// Close the NATS connection
	c.nc.Close()

	// Verify connection is closed
	status := c.nc.Status()
	if status != nats.DISCONNECTED && status != nats.CLOSED {
		c.logger.Error("Failed to close NATS connection properly", "status", status)
		return errors.New("failed to close NATS connection properly")
	}

	c.logger.Info("NATS connection closed successfully")
	c.nc = nil
	return nil
}

// SessionID returns the current session identifier.
// This method implements the mcp.Connection interface.
func (c *Connection) SessionID() string {
	return c.sessionID
}

// getNatsURL returns the NATS URL, using default if not specified
func (t *Transport) getNatsURL() string {
	if t.NatsURL == "" {
		return nats.DefaultURL
	}
	return t.NatsURL
}

// getLogger returns the configured logger or creates a default one
func (t *Transport) getLogger() *slog.Logger {
	if t.Logger != nil {
		return t.Logger
	}
	return slog.Default().With("component", "mcp-nats-transport")
}

// getNatsOptions returns NATS connection options with proper defaults
func (t *Transport) getNatsOptions(logger *slog.Logger) []nats.Option {
	connectTimeout := t.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = 30 * time.Second
	}

	reconnectWait := t.ReconnectWait
	if reconnectWait == 0 {
		reconnectWait = 5 * time.Second
	}

	maxReconnects := t.MaxReconnects
	if maxReconnects == 0 {
		maxReconnects = -1 // Unlimited
	}

	return []nats.Option{
		nats.Timeout(connectTimeout),
		nats.MaxReconnects(maxReconnects),
		nats.ReconnectWait(reconnectWait),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			logger.Warn("NATS connection disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS connection reestablished", "server_url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if err := nc.LastError(); err != nil {
				logger.Error("NATS connection closed with error", "error", err)
			} else {
				logger.Info("NATS connection closed normally")
			}
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			logger.Error("NATS subscription error",
				"subject", sub.Subject,
				"error", err)
		}),
	}
}
