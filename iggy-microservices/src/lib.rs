pub mod client;
pub mod config;
pub mod consumer;
pub mod error;
pub mod producer;

// Re-export commonly used types for convenience
pub use client::{IggyClientWrapper, MessageConsumer, MessageProducer};
pub use config::{
    AuthenticationConfig, ClientConfig, IggyConfig, ServerConfig, StreamConfig, TopicConfig,
};
pub use consumer::{
    ConsumerService, MessageProcessor, SimpleMessageProcessor, create_consumer_service,
};
pub use error::{IggyError, Result};
pub use producer::{
    MessageHandler, ProducerService, SimpleMessageHandler, create_producer_service,
};

// Convenience functions for quick setup
pub async fn create_default_client() -> Result<IggyClientWrapper> {
    let config = IggyConfig::default();
    IggyClientWrapper::new(config).await
}

pub async fn create_client_with_config(config: IggyConfig) -> Result<IggyClientWrapper> {
    IggyClientWrapper::new(config).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_default_config() {
        let config = IggyConfig::default();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 8090);
        assert_eq!(config.authentication.username, "root");
        assert_eq!(config.authentication.password, "iggy");
    }
}
