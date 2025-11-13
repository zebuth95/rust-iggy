use iggy::client::{Client, MessageClient, StreamClient, TopicClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use std::error::Error;

use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let client = IggyClient::default();
    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;
    init_system(&client).await;
    produce_messages(&client).await?;
    Ok(())
}

async fn init_system(client: &IggyClient) {
    match client.create_stream("sample-stream", Some(STREAM_ID)).await {
        Ok(_) => info!("Stream was created."),
        Err(_) => warn!("Stream already exists and will not be created again."),
    }

    match client
        .create_topic(
            &STREAM_ID.try_into().unwrap(),
            "sample-topic",
            1,
            CompressionAlgorithm::default(),
            None,
            Some(TOPIC_ID),
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
    {
        Ok(_) => info!("Topic was created."),
        Err(_) => warn!("Topic already exists and will not be created again."),
    }
}

async fn produce_messages(client: &IggyClient) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        STREAM_ID,
        TOPIC_ID,
        PARTITION_ID,
        interval.as_millis()
    );

    let mut current_id = 0;
    let messages_per_batch = 10;
    let partitioning = Partitioning::partition_id(PARTITION_ID);
    loop {
        let mut messages = Vec::new();
        for _ in 0..messages_per_batch {
            current_id += 1;
            let payload = format!("message-{current_id}");
            let message = Message::new(Some(0), payload.into(), None);
            messages.push(message);
        }
        client
            .send_messages(
                &STREAM_ID.try_into().unwrap(),
                &TOPIC_ID.try_into().unwrap(),
                &partitioning,
                &mut messages,
            )
            .await?;
        info!("Sent {messages_per_batch} message(s).");
        sleep(interval).await;
    }
}
