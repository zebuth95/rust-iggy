use iggy_microservices::{
    Result, create_default_client,
    producer::{SimpleMessageHandler, create_producer_service},
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

    info!("Starting Iggy Producer Example");

    // Create default client
    let client = create_default_client().await?;
    info!("Successfully connected to Iggy server");

    // Create producer service
    let producer_service =
        create_producer_service(client, STREAM_ID, TOPIC_ID, PARTITION_ID).await?;

    info!(
        "Producer service created for stream {}, topic {}, partition {}",
        STREAM_ID, TOPIC_ID, PARTITION_ID
    );

    // Create message handler
    let handler = SimpleMessageHandler::new("example-message".to_string(), Some(MAX_MESSAGES));

    // Run producer with handler
    info!("Starting to produce {} messages...", MAX_MESSAGES);
    producer_service.run_with_handler(handler).await?;

    info!("Producer example completed successfully");
    Ok(())
}
