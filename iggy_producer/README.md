# Multi-Tenant Producer

A Rust-based multi-tenant producer for the Iggy message streaming system that demonstrates how to create isolated tenant environments with separate streams, users, and permissions.

## Overview

This producer creates multiple tenants, each with:
- Dedicated stream
- Dedicated user account with specific permissions
- Multiple producers per tenant
- Multiple topics per stream (events, logs, notifications)
- Configurable number of partitions

## Configuration

The producer can be configured using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `TENANTS_COUNT` | Number of tenants to create | 3 |
| `PRODUCERS_COUNT` | Number of producers per tenant | 3 |
| `PARTITIONS_COUNT` | Number of partitions per topic | 3 |
| `ENSURE_ACCESS` | Verify tenant isolation (true/false) | true |

## Usage

### Basic Usage
```bash
cargo run
```

### With Custom Configuration
```bash
export TENANTS_COUNT=5
export PRODUCERS_COUNT=2
export PARTITIONS_COUNT=1
export ENSURE_ACCESS=false
cargo run
```

### With Custom Server Address
```bash
cargo run -- --tcp-server-address 192.168.1.100:8090
```

## Architecture

### Tenant Structure
Each tenant gets:
- Stream: `tenant_{id}_stream`
- User: `tenant_{id}_producer`
- Topics: `events`, `logs`, `notifications`
- Dedicated producers for each topic

### Security Model
- Each tenant user only has access to their own stream
- Root user creates streams and users initially
- Tenant isolation is verified if `ENSURE_ACCESS=true`

### Producer Behavior
- Each producer sends messages to its assigned topic
- Messages follow format: `{type}-{producer_id}-{message_id}`
- Producers run concurrently using Tokio tasks
- Configurable batch size and intervals

## Example Output

```
--- Multi-tenant producer has started, tenants: 3, producers: 3, partitions: 3 ---

--- Creating root client to manage streams and users ---

--- Creating streams and users with permissions for each tenant ---
Created stream: tenant_1_stream with ID: 1
Created user: tenant_1_producer with ID: 2, with permissions for stream: tenant_1_stream

--- Creating clients for each tenant ---

--- Ensuring access to streams for each tenant ---
Ensured access to stream: tenant_1_stream
Ensured no access to stream: tenant_2_stream
Ensured no access to stream: tenant_3_stream

--- Starting 3 producer(s) for each tenant ---
Sent: 10 message(s) by tenant: 1, producer: 1, to: tenant_1_stream -> events
```

## Dependencies

- `iggy`: Iggy client library
- `tokio`: Async runtime
- `tracing`: Structured logging
- `ahash`: High-performance hash maps
- `futures-util`: Async utilities

## Use Cases

- Multi-tenant SaaS applications
- Isolated customer environments
- Testing stream isolation
- Performance testing with multiple producers
- Security validation for tenant separation