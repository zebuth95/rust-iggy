# Iggy Microservices Library

A modular, async trait-based Rust library for building microservices with Iggy message streaming.

## Overview

This library provides a clean, modular architecture for building microservices that use Iggy as their message streaming backbone. It features async traits, comprehensive error handling, and flexible configuration for easy integration into multiple microservices.

## Features

- **Async Trait Architecture**: Clean separation of concerns with `MessageProducer` and `MessageConsumer` traits
- **Modular Design**: Separate modules for client, producer, consumer, and configuration
- **Flexible Configuration**: JSON-based configuration with sensible defaults
- **Multiple Topics Support**: Easy management of multiple streams and topics
- **Error Handling**: Comprehensive error types with `thiserror` integration
- **JSON Serialization**: Built-in support for JSON message serialization/deserialization
- **Auto-reconnection**: Automatic reconnection with configurable retry logic
- **Tracing Integration**: Built-in logging with `tracing` framework

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
iggy-microservices = { path = "./iggy-microservices" }
```

### Basic Usage

```rust
use iggy_microservices::{Result, create_default_client};
use iggy_microservices::producer::{ProducerService, SimpleMessageHandler, create_producer_service};
use iggy_microservices::consumer::{ConsumerService, SimpleMessageProcessor, create_consumer_service};

#[tokio::main]
async fn main() -> Result<()> {
    // Create client
    let client = create_default_client().await?;
    
    // Producer example
    let producer = create_producer_service(client.clone(), 1, 1, 1).await?;
    let handler = SimpleMessageHandler::new("message".to_string(), Some(10));
    producer.run_with_handler(handler).await?;
    
    // Consumer example
    let consumer = create_consumer_service(client, 1, 1, Some(1), true).await?;
    let processor = SimpleMessageProcessor::new(Some(10));
    consumer.run_with_processor(processor, 0).await?;
    
    Ok(())
}
```

## Architecture

### Core Traits

#### MessageProducer
```rust
#[async_trait]
pub trait MessageProducer: Send + Sync {
    async fn send_message(&self, stream_id: u32, topic_id: u32, partition_id: u32, payload: Vec<u8>) -> Result<()>;
    async fn send_messages_batch(&self, stream_id: u32, topic_id: u32, partition_id: u32, messages: Vec<Vec<u8>>) -> Result<()>;
    async fn ensure_stream_exists(&self, stream_id: u32, stream_name: &str) -> Result<()>;
    async fn ensure_topic_exists(&self, stream_id: u32, topic_id: u32, topic_name: &str, partitions_count: u32) -> Result<()>;
}
```

#### MessageConsumer
```rust
#[async_trait]
pub trait MessageConsumer: Send + Sync {
    async fn poll_messages(&self, stream_id: u32, topic_id: u32, partition_id: Option<u32>, offset: u64, count: u32) -> Result<PolledMessages>;
    async fn commit_offset(&self, stream_id: u32, topic_id: u32, partition_id: Option<u32>, offset: u64) -> Result<()>;
}
```

### MessageHandler & MessageProcessor

Implement these traits to define custom message generation and processing logic:

```rust
struct CustomHandler;
struct CustomProcessor;

#[async_trait]
impl MessageHandler for CustomHandler {
    async fn generate_message(&self) -> Result<Vec<u8>> {
        // Generate your message payload
        Ok(b"custom message".to_vec())
    }
    
    fn should_continue(&self) -> bool {
        // Control when to stop producing
        true
    }
}

#[async_trait]
impl MessageProcessor for CustomProcessor {
    async fn process_message(&self, payload: &[u8]) -> Result<()> {
        // Process the message
        println!("Received: {}", String::from_utf8_lossy(payload));
        Ok(())
    }
    
    fn should_continue(&self) -> bool {
        // Control when to stop consuming
        true
    }
}
```

## Configuration

### Default Configuration

The library provides sensible defaults:

- **Server**: 127.0.0.1:8090
- **Authentication**: root/iggy
- **Poll Interval**: 500ms
- **Batch Size**: 10 messages
- **Auto-create**: Streams and topics created automatically

### Custom Configuration

```rust
use iggy_microservices::config::{IggyConfig, ServerConfig, AuthenticationConfig, StreamConfig, TopicConfig, ClientConfig};
use std::time::Duration;

let config = IggyConfig {
    server: ServerConfig {
        host: "localhost".to_string(),
        port: 8090,
        tcp_enabled: true,
        quic_enabled: false,
    },
    authentication: AuthenticationConfig {
        username: "admin".to_string(),
        password: "password".to_string(),
    },
    streams: vec![
        StreamConfig {
            id: 1,
            name: "my-stream".to_string(),
            topics: vec![
                TopicConfig {
                    id: 1,
                    name: "my-topic".to_string(),
                    partitions_count: 3,
                    message_expiry: None,
                    max_topic_size: None,
                }
            ],
        }
    ],
    client: ClientConfig {
        poll_interval: Duration::from_millis(100),
        batch_size: 5,
        auto_create_streams: true,
        auto_create_topics: true,
        reconnect_attempts: 5,
        reconnect_delay: Duration::from_secs(2),
    },
};
```

### JSON Configuration

Load configuration from JSON file:

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 8090,
    "tcp_enabled": true,
    "quic_enabled": false
  },
  "authentication": {
    "username": "root",
    "password": "iggy"
  },
  "streams": [
    {
      "id": 1,
      "name": "user-events",
      "topics": [
        {
          "id": 1,
          "name": "user-registrations",
          "partitions_count": 2,
          "message_expiry": null,
          "max_topic_size": null
        }
      ]
    }
  ],
  "client": {
    "poll_interval": 500,
    "batch_size": 10,
    "auto_create_streams": true,
    "auto_create_topics": true,
    "reconnect_attempts": 3,
    "reconnect_delay": 1000
  }
}
```

## Examples

### Basic Producer/Consumer

```bash
# Run producer example
cargo run --bin producer-example

# Run consumer example  
cargo run --bin consumer-example
```

### Advanced Multi-topic Example

```bash
cargo run --example advanced_example
```

### Custom Message Types

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    event_type: String,
    timestamp: u64,
}

// Send JSON messages
producer_service.send_json_message(&UserEvent {
    user_id: "123".to_string(),
    event_type: "registered".to_string(),
    timestamp: 1234567890,
}).await?;

// Process JSON messages
consumer_service.process_json_messages::<UserEvent, _>(processor, 0).await?;
```

## Error Handling

The library uses a custom `IggyError` enum with `thiserror`:

```rust
use iggy_microservices::error::{IggyError, Result};

match producer.send_message(1, 1, 1, payload).await {
    Ok(_) => println!("Message sent"),
    Err(IggyError::ConnectionError(msg)) => println!("Connection failed: {}", msg),
    Err(e) => println!("Other error: {}", e),
}
```

## Running Tests

```bash
cargo test
```

## Dependencies

- `iggy` - Iggy message streaming client
- `tokio` - Async runtime
- `async-trait` - Async trait support
- `serde` - Serialization/deserialization
- `tracing` - Structured logging
- `thiserror` - Error handling
- `config` - Configuration management

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run `cargo test`
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.