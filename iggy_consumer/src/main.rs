use iggy::client::{Client, MessageClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::models::messages::PolledMessage;
use iggy::users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

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
    consume_messages(&client).await
}

async fn consume_messages(client: &IggyClient) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    info!(
        "Messages will be consumed from stream: {}, topic: {}, partition: {} with interval {} ms.",
        STREAM_ID,
        TOPIC_ID,
        PARTITION_ID,
        interval.as_millis()
    );

    let mut offset = 0;
    let messages_per_batch = 10;
    let consumer = Consumer::default();
    let mut strategy = PollingStrategy::offset(offset);
    loop {
        let polled_messages = client
            .poll_messages(
                &STREAM_ID.try_into()?,
                &TOPIC_ID.try_into()?,
                Some(PARTITION_ID),
                &consumer,
                &PollingStrategy::offset(offset),
                messages_per_batch,
                false,
            )
            .await?;

        if polled_messages.messages.is_empty() {
            info!("No messages found.");
            sleep(interval).await;
            continue;
        }

        offset += polled_messages.messages.len() as u64;
        strategy.set_value(offset);
        for message in polled_messages.messages {
            handle_message(&message)?;
        }
        sleep(interval).await;
    }
}

fn handle_message(message: &PolledMessage) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a simple string.
    let payload = std::str::from_utf8(&message.payload)?;
    info!(
        "Handling message at offset: {}, payload: {}...",
        message.offset, payload
    );
    Ok(())
}
