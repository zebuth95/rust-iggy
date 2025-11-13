use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IggyConfig {
    pub server: ServerConfig,
    pub authentication: AuthenticationConfig,
    pub streams: Vec<StreamConfig>,
    pub client: ClientConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub tcp_enabled: bool,
    pub quic_enabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8090,
            tcp_enabled: true,
            quic_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationConfig {
    pub username: String,
    pub password: String,
}

impl Default for AuthenticationConfig {
    fn default() -> Self {
        Self {
            username: "root".to_string(),
            password: "iggy".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub id: u32,
    pub name: String,
    pub topics: Vec<TopicConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub id: u32,
    pub name: String,
    pub partitions_count: u32,
    pub message_expiry: Option<Duration>,
    pub max_topic_size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub auto_create_streams: bool,
    pub auto_create_topics: bool,
    pub reconnect_attempts: u32,
    pub reconnect_delay: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(500),
            batch_size: 10,
            auto_create_streams: true,
            auto_create_topics: true,
            reconnect_attempts: 3,
            reconnect_delay: Duration::from_secs(1),
        }
    }
}

impl Default for IggyConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            authentication: AuthenticationConfig::default(),
            streams: vec![StreamConfig {
                id: 1,
                name: "default-stream".to_string(),
                topics: vec![TopicConfig {
                    id: 1,
                    name: "default-topic".to_string(),
                    partitions_count: 1,
                    message_expiry: None,
                    max_topic_size: None,
                }],
            }],
            client: ClientConfig::default(),
        }
    }
}
