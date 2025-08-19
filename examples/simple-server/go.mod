module github.com/ganawaj/mcp-transport-nats/examples/simple-server

go 1.24.1

require (
	github.com/ganawaj/mcp-transport-nats v0.1.0
	github.com/modelcontextprotocol/go-sdk v0.2.1-0.20250818184902-75f999959014
)

require (
	github.com/google/jsonschema-go v0.2.0 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
)

replace github.com/ganawaj/mcp-transport-nats => ../..
