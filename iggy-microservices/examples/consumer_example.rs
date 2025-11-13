use iggy_microservices::{
    Result,
    consumer::{SimpleMessageProcessor, create_consumer_service},
    create_default_client,
};

use tracing::{Level, info};
use tracing_subscriber;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
const MAX_MESSAGES: u64 = 100;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Iggy Consumer Example");

    // Create default client
    let client = create_default_client().await?;
    info!("Successfully connected to Iggy server");

    // Create consumer service
    let consumer_service = create_consumer_service(
        client,
        STREAM_ID,
        TOPIC_ID,
        Some(PARTITION_ID),
        true, // auto-commit offsets
    )
    .await?;

    info!(
        "Consumer service created for stream {}, topic {}, partition {}",
        STREAM_ID, TOPIC_ID, PARTITION_ID
    );

    // Create message processor
    let processor = SimpleMessageProcessor::new(Some(MAX_MESSAGES));

    // Run consumer with processor
    info!("Starting to consume messages (max: {})...", MAX_MESSAGES);
    consumer_service.run_with_processor(processor, 0).await?;

    info!("Consumer example completed successfully");
    Ok(())
}
