use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::client::{IggyClientWrapper, MessageConsumer};
use crate::error::Result;

#[async_trait]
pub trait MessageProcessor: Send + Sync {
    async fn process_message(&self, payload: &[u8]) -> Result<()>;
    fn should_continue(&self) -> bool;
}

pub struct ConsumerService<C: MessageConsumer> {
    consumer: C,
    stream_id: u32,
    topic_id: u32,
    partition_id: Option<u32>,
    batch_size: u32,
    poll_interval: Duration,
    auto_commit: bool,
}

impl<C: MessageConsumer> ConsumerService<C> {
    pub fn new(
        consumer: C,
        stream_id: u32,
        topic_id: u32,
        partition_id: Option<u32>,
        batch_size: u32,
        poll_interval: Duration,
        auto_commit: bool,
    ) -> Self {
        Self {
            consumer,
            stream_id,
            topic_id,
            partition_id,
            batch_size,
            poll_interval,
            auto_commit,
        }
    }

    pub async fn run_with_processor<P>(&self, processor: P, start_offset: u64) -> Result<()>
    where
        P: MessageProcessor,
    {
        info!(
            "Starting consumer service for stream {}, topic {}, partition {:?}",
            self.stream_id, self.topic_id, self.partition_id
        );

        let mut current_offset = start_offset;

        while processor.should_continue() {
            match self
                .consumer
                .poll_messages(
                    self.stream_id,
                    self.topic_id,
                    self.partition_id,
                    current_offset,
                    self.batch_size,
                )
                .await
            {
                Ok(polled_messages) => {
                    if polled_messages.messages.is_empty() {
                        debug!("No messages found at offset {}", current_offset);
                        sleep(self.poll_interval).await;
                        continue;
                    }

                    let messages_count = polled_messages.messages.len();
                    debug!("Received {} messages", messages_count);

                    for message in polled_messages.messages {
                        match processor.process_message(&message.payload).await {
                            Ok(_) => {
                                debug!(
                                    "Successfully processed message at offset {}",
                                    message.offset
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to process message at offset {}: {}",
                                    message.offset, e
                                );
                            }
                        }

                        current_offset = message.offset + 1;
                    }

                    if self.auto_commit {
                        if let Err(e) = self
                            .consumer
                            .commit_offset(
                                self.stream_id,
                                self.topic_id,
                                self.partition_id,
                                current_offset,
                            )
                            .await
                        {
                            warn!("Failed to commit offset {}: {}", current_offset, e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to poll messages: {}", e);
                    sleep(self.poll_interval).await;
                }
            }

            sleep(self.poll_interval).await;
        }

        info!("Consumer service stopped");
        Ok(())
    }

    pub async fn process_json_messages<T, P>(&self, processor: P, start_offset: u64) -> Result<()>
    where
        T: DeserializeOwned + serde::Serialize,
        P: MessageProcessor,
    {
        info!(
            "Starting JSON consumer service for stream {}, topic {}, partition {:?}",
            self.stream_id, self.topic_id, self.partition_id
        );

        let mut current_offset = start_offset;

        while processor.should_continue() {
            match self
                .consumer
                .poll_messages(
                    self.stream_id,
                    self.topic_id,
                    self.partition_id,
                    current_offset,
                    self.batch_size,
                )
                .await
            {
                Ok(polled_messages) => {
                    if polled_messages.messages.is_empty() {
                        debug!("No messages found at offset {}", current_offset);
                        sleep(self.poll_interval).await;
                        continue;
                    }

                    let messages_count = polled_messages.messages.len();
                    debug!("Received {} messages", messages_count);

                    for message in polled_messages.messages {
                        match serde_json::from_slice::<T>(&message.payload) {
                            Ok(deserialized) => {
                                let json_payload = serde_json::to_vec(&deserialized)?;
                                match processor.process_message(&json_payload).await {
                                    Ok(_) => {
                                        debug!(
                                            "Successfully processed JSON message at offset {}",
                                            message.offset
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to process JSON message at offset {}: {}",
                                            message.offset, e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to deserialize JSON message at offset {}: {}",
                                    message.offset, e
                                );
                            }
                        }

                        current_offset = message.offset + 1;
                    }

                    if self.auto_commit {
                        if let Err(e) = self
                            .consumer
                            .commit_offset(
                                self.stream_id,
                                self.topic_id,
                                self.partition_id,
                                current_offset,
                            )
                            .await
                        {
                            warn!("Failed to commit offset {}: {}", current_offset, e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to poll messages: {}", e);
                    sleep(self.poll_interval).await;
                }
            }

            sleep(self.poll_interval).await;
        }

        info!("JSON consumer service stopped");
        Ok(())
    }

    pub fn get_stream_id(&self) -> u32 {
        self.stream_id
    }

    pub fn get_topic_id(&self) -> u32 {
        self.topic_id
    }

    pub fn get_partition_id(&self) -> Option<u32> {
        self.partition_id
    }

    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.auto_commit = auto_commit;
    }
}

pub struct SimpleMessageProcessor {
    max_messages: Option<u64>,
    processed_count: u64,
}

impl SimpleMessageProcessor {
    pub fn new(max_messages: Option<u64>) -> Self {
        Self {
            max_messages,
            processed_count: 0,
        }
    }
}

#[async_trait]
impl MessageProcessor for SimpleMessageProcessor {
    async fn process_message(&self, payload: &[u8]) -> Result<()> {
        let message_str = std::str::from_utf8(payload).map_err(|e| {
            crate::error::IggyError::MessageProcessingError(format!("Invalid UTF-8: {}", e))
        })?;

        info!("Processing message: {}", message_str);
        Ok(())
    }

    fn should_continue(&self) -> bool {
        match self.max_messages {
            Some(max) => self.processed_count < max,
            None => true,
        }
    }
}

impl SimpleMessageProcessor {
    pub fn increment_processed_count(&mut self) {
        self.processed_count += 1;
    }

    pub fn get_processed_count(&self) -> u64 {
        self.processed_count
    }
}

pub async fn create_consumer_service(
    client_wrapper: IggyClientWrapper,
    stream_id: u32,
    topic_id: u32,
    partition_id: Option<u32>,
    auto_commit: bool,
) -> Result<ConsumerService<IggyClientWrapper>> {
    let config = client_wrapper.get_config().clone();

    Ok(ConsumerService::new(
        client_wrapper,
        stream_id,
        topic_id,
        partition_id,
        config.client.batch_size as u32,
        config.client.poll_interval,
        auto_commit,
    ))
}
