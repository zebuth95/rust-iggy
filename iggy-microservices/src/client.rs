use async_trait::async_trait;
use iggy::client::{
    Client, ConsumerOffsetClient, MessageClient, StreamClient, TopicClient, UserClient,
};
use iggy::clients::client::IggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::models::messages::PolledMessages;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::config::IggyConfig;
use crate::error::{IggyError, Result};

#[async_trait]
pub trait MessageProducer: Send + Sync {
    async fn send_message(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        payload: Vec<u8>,
    ) -> Result<()>;

    async fn send_messages_batch(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        messages: Vec<Vec<u8>>,
    ) -> Result<()>;

    async fn ensure_stream_exists(&self, stream_id: u32, stream_name: &str) -> Result<()>;
    async fn ensure_topic_exists(
        &self,
        stream_id: u32,
        topic_id: u32,
        topic_name: &str,
        partitions_count: u32,
    ) -> Result<()>;
}

#[async_trait]
pub trait MessageConsumer: Send + Sync {
    async fn poll_messages(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: Option<u32>,
        offset: u64,
        count: u32,
    ) -> Result<PolledMessages>;

    async fn commit_offset(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<()>;
}

pub struct IggyClientWrapper {
    client: Arc<Mutex<IggyClient>>,
    config: IggyConfig,
}

impl IggyClientWrapper {
    pub async fn new(config: IggyConfig) -> Result<Self> {
        let client = IggyClient::default();

        // Connect to server
        client.connect().await.map_err(|e| {
            IggyError::ConnectionError(format!("Failed to connect to Iggy server: {}", e))
        })?;

        // Authenticate
        client
            .login_user(
                &config.authentication.username,
                &config.authentication.password,
            )
            .await
            .map_err(|e| {
                IggyError::AuthenticationError(format!("Failed to authenticate: {}", e))
            })?;

        info!(
            "Connected to Iggy server at {}:{}",
            config.server.host, config.server.port
        );

        let wrapper = Self {
            client: Arc::new(Mutex::new(client)),
            config,
        };

        // Initialize streams and topics if auto-creation is enabled
        if wrapper.config.client.auto_create_streams {
            wrapper.initialize_streams().await?;
        }

        Ok(wrapper)
    }

    async fn initialize_streams(&self) -> Result<()> {
        for stream_config in &self.config.streams {
            self.ensure_stream_exists(stream_config.id, &stream_config.name)
                .await?;

            if self.config.client.auto_create_topics {
                for topic_config in &stream_config.topics {
                    self.ensure_topic_exists(
                        stream_config.id,
                        topic_config.id,
                        &topic_config.name,
                        topic_config.partitions_count,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    pub fn get_config(&self) -> &IggyConfig {
        &self.config
    }

    pub async fn reconnect(&self) -> Result<()> {
        let client = self.client.lock().await;

        for attempt in 1..=self.config.client.reconnect_attempts {
            info!(
                "Attempting to reconnect to Iggy server (attempt {}/{})",
                attempt, self.config.client.reconnect_attempts
            );

            match client.connect().await {
                Ok(_) => {
                    info!("Successfully reconnected to Iggy server");
                    return Ok(());
                }
                Err(e) => {
                    error!("Reconnection attempt {} failed: {}", attempt, e);
                    if attempt < self.config.client.reconnect_attempts {
                        tokio::time::sleep(self.config.client.reconnect_delay).await;
                    }
                }
            }
        }

        Err(IggyError::ConnectionError(
            "Failed to reconnect to Iggy server after all attempts".to_string(),
        ))
    }
}

#[async_trait]
impl MessageProducer for IggyClientWrapper {
    async fn send_message(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        payload: Vec<u8>,
    ) -> Result<()> {
        let message = Message::new(None, payload.into(), None);
        let partitioning = Partitioning::partition_id(partition_id);

        let client = self.client.lock().await;
        client
            .send_messages(
                &stream_id.try_into().unwrap(),
                &topic_id.try_into().unwrap(),
                &partitioning,
                &mut vec![message],
            )
            .await
            .map_err(|e| {
                IggyError::MessageProcessingError(format!("Failed to send message: {}", e))
            })?;

        debug!(
            "Sent message to stream {}, topic {}, partition {}",
            stream_id, topic_id, partition_id
        );
        Ok(())
    }

    async fn send_messages_batch(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        messages: Vec<Vec<u8>>,
    ) -> Result<()> {
        let mut iggy_messages: Vec<Message> = messages
            .into_iter()
            .map(|payload| Message::new(None, payload.into(), None))
            .collect();

        let partitioning = Partitioning::partition_id(partition_id);

        let client = self.client.lock().await;
        client
            .send_messages(
                &stream_id.try_into().unwrap(),
                &topic_id.try_into().unwrap(),
                &partitioning,
                &mut iggy_messages,
            )
            .await
            .map_err(|e| {
                IggyError::MessageProcessingError(format!("Failed to send messages batch: {}", e))
            })?;

        debug!(
            "Sent {} messages to stream {}, topic {}, partition {}",
            iggy_messages.len(),
            stream_id,
            topic_id,
            partition_id
        );
        Ok(())
    }

    async fn ensure_stream_exists(&self, stream_id: u32, stream_name: &str) -> Result<()> {
        let client = self.client.lock().await;

        match client.create_stream(stream_name, Some(stream_id)).await {
            Ok(_) => {
                info!("Created stream '{}' with ID {}", stream_name, stream_id);
                Ok(())
            }
            Err(_) => {
                debug!(
                    "Stream '{}' with ID {} already exists",
                    stream_name, stream_id
                );
                Ok(())
            }
        }
    }

    async fn ensure_topic_exists(
        &self,
        stream_id: u32,
        topic_id: u32,
        topic_name: &str,
        partitions_count: u32,
    ) -> Result<()> {
        let client = self.client.lock().await;

        match client
            .create_topic(
                &stream_id.try_into().unwrap(),
                topic_name,
                partitions_count,
                CompressionAlgorithm::default(),
                None,
                Some(topic_id),
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await
        {
            Ok(_) => {
                info!(
                    "Created topic '{}' with ID {} in stream {}",
                    topic_name, topic_id, stream_id
                );
                Ok(())
            }
            Err(_) => {
                debug!(
                    "Topic '{}' with ID {} already exists in stream {}",
                    topic_name, topic_id, stream_id
                );
                Ok(())
            }
        }
    }
}

#[async_trait]
impl MessageConsumer for IggyClientWrapper {
    async fn poll_messages(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: Option<u32>,
        offset: u64,
        count: u32,
    ) -> Result<PolledMessages> {
        let client = self.client.lock().await;
        let consumer = iggy::consumer::Consumer::default();
        let strategy = PollingStrategy::offset(offset);

        let messages = client
            .poll_messages(
                &stream_id.try_into().unwrap(),
                &topic_id.try_into().unwrap(),
                partition_id,
                &consumer,
                &strategy,
                count,
                false,
            )
            .await
            .map_err(|e| {
                IggyError::MessageProcessingError(format!("Failed to poll messages: {}", e))
            })?;

        debug!(
            "Polled {} messages from stream {}, topic {}",
            messages.messages.len(),
            stream_id,
            topic_id
        );
        Ok(messages)
    }

    async fn commit_offset(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<()> {
        let client = self.client.lock().await;
        let consumer = iggy::consumer::Consumer::default();

        client
            .store_consumer_offset(
                &consumer,
                &stream_id.try_into().unwrap(),
                &topic_id.try_into().unwrap(),
                partition_id,
                offset,
            )
            .await
            .map_err(|e| {
                IggyError::MessageProcessingError(format!("Failed to commit offset: {}", e))
            })?;

        debug!(
            "Committed offset {} for stream {}, topic {}",
            offset, stream_id, topic_id
        );
        Ok(())
    }
}

impl Clone for IggyClientWrapper {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            config: self.config.clone(),
        }
    }
}
