use thiserror::Error;

#[derive(Error, Debug)]
pub enum IggyError {
    #[error("Iggy client error: {0}")]
    ClientError(#[from] iggy::error::IggyError),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Authentication error: {0}")]
    AuthenticationError(String),

    #[error("Stream error: {0}")]
    StreamError(String),

    #[error("Topic error: {0}")]
    TopicError(String),

    #[error("Message processing error: {0}")]
    MessageProcessingError(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

pub type Result<T> = std::result::Result<T, IggyError>;
