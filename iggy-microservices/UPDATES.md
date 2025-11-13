New Modular Async Architecture

I've transformed your working producer and consumer applications into a comprehensive, modular library with async trait architecture that's perfect for multiple microservices.

### Key Features:

1. **Async Trait Architecture**: Clean separation with `MessageProducer` and `MessageConsumer` traits
2. **Modular Design**: Separate modules for client, producer, consumer, and configuration
3. **Flexible Configuration**: JSON-based configuration with sensible defaults
4. **Multiple Topics Support**: Easy management of multiple streams and topics
5. **Error Handling**: Comprehensive error types with `thiserror` integration
6. **JSON Serialization**: Built-in support for JSON message serialization/deserialization
7. **Auto-reconnection**: Automatic reconnection with configurable retry logic
8. **Tracing Integration**: Built-in logging with `tracing` framework

### Project Structure:

```
rust-iggy/iggy-microservices/
├── src/
│   ├── client.rs      # Async trait-based Iggy client wrapper
│   ├── producer.rs    # Producer service with MessageHandler trait
│   ├── consumer.rs    # Consumer service with MessageProcessor trait
│   ├── config.rs      # Configuration structures
│   ├── error.rs       # Error types and handling
│   └── lib.rs         # Main library module
├── examples/
│   ├── producer_example.rs     # Basic producer usage
│   ├── consumer_example.rs     # Basic consumer usage
│   └── advanced_example.rs     # Multi-topic microservices example
├── config.example.json         # Example configuration file
└── README.md                   # Comprehensive documentation
```

### Core Components:

- **`IggyClientWrapper`**: Main client that implements both `MessageProducer` and `MessageConsumer` traits
- **`ProducerService`**: Service for producing messages with configurable handlers
- **`ConsumerService`**: Service for consuming messages with configurable processors
- **`IggyConfig`**: Flexible configuration supporting multiple streams and topics

### Usage Example:

```rust
use iggy_microservices::{Result, create_default_client};
use iggy_microservices::producer::{SimpleMessageHandler, create_producer_service};
use iggy_microservices::consumer::{SimpleMessageProcessor, create_consumer_service};

#[tokio::main]
async fn main() -> Result<()> {
    let client = create_default_client().await?;
    
    // Producer
    let producer = create_producer_service(client.clone(), 1, 1, 1).await?;
    let handler = SimpleMessageHandler::new("message".to_string(), Some(10));
    producer.run_with_handler(handler).await?;
    
    // Consumer
    let consumer = create_consumer_service(client, 1, 1, Some(1), true).await?;
    let processor = SimpleMessageProcessor::new(Some(10));
    consumer.run_with_processor(processor, 0).await?;
    
    Ok(())
}
```

### Benefits Over Original Code:

1. **Modularity**: Each component is isolated and can be used independently
2. **Extensibility**: Easy to implement custom message handlers and processors
3. **Configuration**: Centralized configuration management
4. **Error Handling**: Comprehensive error types and handling
5. **Observability**: Built-in tracing and logging
6. **Reusability**: Can be easily integrated into multiple microservices
7. **Testability**: Modular design makes testing easier

The library is now ready to be used across multiple microservices with different topics and streams, providing a clean, async, and modular foundation for your Iggy-based messaging infrastructure.
