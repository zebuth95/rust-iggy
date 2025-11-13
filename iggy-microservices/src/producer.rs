use async_trait::async_trait;
use serde::Serialize;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::client::{IggyClientWrapper, MessageProducer};
use crate::error::Result;

#[async_trait]
pub trait MessageHandler: Send + Sync {
    async fn generate_message(&self) -> Result<Vec<u8>>;
    fn should_continue(&self) -> bool;
}

pub struct ProducerService<P: MessageProducer> {
    producer: P,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    batch_size: usize,
    poll_interval: Duration,
}

impl<P: MessageProducer> ProducerService<P> {
    pub fn new(
        producer: P,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        batch_size: usize,
        poll_interval: Duration,
    ) -> Self {
        Self {
            producer,
            stream_id,
            topic_id,
            partition_id,
            batch_size,
            poll_interval,
        }
    }

    pub async fn run_with_handler<H>(&self, handler: H) -> Result<()>
    where
        H: MessageHandler,
    {
        info!(
            "Starting producer service for stream {}, topic {}, partition {}",
            self.stream_id, self.topic_id, self.partition_id
        );

        while handler.should_continue() {
            let mut messages = Vec::with_capacity(self.batch_size);

            for _ in 0..self.batch_size {
                if !handler.should_continue() {
                    break;
                }

                match handler.generate_message().await {
                    Ok(message) => messages.push(message),
                    Err(e) => {
                        warn!("Failed to generate message: {}", e);
                        continue;
                    }
                }
            }

            if !messages.is_empty() {
                match self
                    .producer
                    .send_messages_batch(self.stream_id, self.topic_id, self.partition_id, messages)
                    .await
                {
                    Ok(_) => {
                        debug!("Successfully sent batch of {} messages", self.batch_size);
                    }
                    Err(e) => {
                        warn!("Failed to send messages batch: {}", e);
                    }
                }
            }

            sleep(self.poll_interval).await;
        }

        info!("Producer service stopped");
        Ok(())
    }

    pub async fn send_single_message(&self, payload: Vec<u8>) -> Result<()> {
        self.producer
            .send_message(self.stream_id, self.topic_id, self.partition_id, payload)
            .await
    }

    pub async fn send_json_message<T: Serialize>(&self, data: &T) -> Result<()> {
        let payload = serde_json::to_vec(data)?;
        self.send_single_message(payload).await
    }

    pub fn get_stream_id(&self) -> u32 {
        self.stream_id
    }

    pub fn get_topic_id(&self) -> u32 {
        self.topic_id
    }

    pub fn get_partition_id(&self) -> u32 {
        self.partition_id
    }
}

pub struct SimpleMessageHandler {
    message_counter: u64,
    max_messages: Option<u64>,
    message_template: String,
}

impl SimpleMessageHandler {
    pub fn new(message_template: String, max_messages: Option<u64>) -> Self {
        Self {
            message_counter: 0,
            max_messages,
            message_template,
        }
    }
}

#[async_trait]
impl MessageHandler for SimpleMessageHandler {
    async fn generate_message(&self) -> Result<Vec<u8>> {
        let message = format!("{}-{}", self.message_template, self.message_counter + 1);
        Ok(message.into_bytes())
    }

    fn should_continue(&self) -> bool {
        match self.max_messages {
            Some(max) => (self.message_counter + 1) <= max,
            None => true,
        }
    }
}

impl SimpleMessageHandler {
    pub fn increment_counter(&mut self) {
        self.message_counter += 1;
    }
}

pub async fn create_producer_service(
    client_wrapper: IggyClientWrapper,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
) -> Result<ProducerService<IggyClientWrapper>> {
    let config = client_wrapper.get_config().clone();

    Ok(ProducerService::new(
        client_wrapper,
        stream_id,
        topic_id,
        partition_id,
        config.client.batch_size,
        config.client.poll_interval,
    ))
}
