use iggy_microservices::{
    IggyError, Result,
    config::{IggyConfig, StreamConfig, TopicConfig},
    consumer::{MessageProcessor, create_consumer_service},
    create_client_with_config,
    producer::{MessageHandler, create_producer_service},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{Level, info, warn};
use tracing_subscriber;

// Custom message types for different microservices
#[derive(Debug, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    event_type: String,
    timestamp: u64,
    data: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct OrderEvent {
    order_id: String,
    customer_id: String,
    total_amount: f64,
    items: Vec<String>,
    status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Notification {
    recipient: String,
    message: String,
    notification_type: String,
    priority: u8,
}

// Custom message handler for user events
struct UserEventHandler {
    counter: Arc<Mutex<u64>>,
    max_events: Option<u64>,
}

impl UserEventHandler {
    fn new(max_events: Option<u64>) -> Self {
        Self {
            counter: Arc::new(Mutex::new(0)),
            max_events,
        }
    }
}

#[async_trait::async_trait]
impl MessageHandler for UserEventHandler {
    async fn generate_message(&self) -> Result<Vec<u8>> {
        let mut counter = self.counter.lock().await;
        *counter += 1;

        let event = UserEvent {
            user_id: format!("user_{}", counter),
            event_type: "user_registered".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            data: serde_json::json!({
                "email": format!("user{}@example.com", counter),
                "name": format!("User {}", counter)
            }),
        };

        let payload = serde_json::to_vec(&event)?;
        info!("Generated user event: {:?}", event);
        Ok(payload)
    }

    fn should_continue(&self) -> bool {
        let counter = futures::executor::block_on(async { *self.counter.lock().await });
        match self.max_events {
            Some(max) => counter < max,
            None => true,
        }
    }
}

// Custom message processor for order events
struct OrderEventProcessor {
    processed_count: Arc<Mutex<u64>>,
    max_events: Option<u64>,
}

impl OrderEventProcessor {
    fn new(max_events: Option<u64>) -> Self {
        Self {
            processed_count: Arc::new(Mutex::new(0)),
            max_events,
        }
    }
}

#[async_trait::async_trait]
impl MessageProcessor for OrderEventProcessor {
    async fn process_message(&self, payload: &[u8]) -> Result<()> {
        match serde_json::from_slice::<OrderEvent>(payload) {
            Ok(order_event) => {
                let mut count = self.processed_count.lock().await;
                *count += 1;

                info!(
                    "Processed order event #{}, Order ID: {}, Customer: {}, Total: ${:.2}, Status: {}",
                    count,
                    order_event.order_id,
                    order_event.customer_id,
                    order_event.total_amount,
                    order_event.status
                );

                // Simulate business logic
                if order_event.status == "completed" {
                    info!("Order {} completed successfully!", order_event.order_id);
                }
            }
            Err(e) => {
                warn!("Failed to deserialize order event: {}", e);
            }
        }
        Ok(())
    }

    fn should_continue(&self) -> bool {
        let count = futures::executor::block_on(async { *self.processed_count.lock().await });
        match self.max_events {
            Some(max) => count < max,
            None => true,
        }
    }
}

// Custom message processor for notifications
struct NotificationProcessor {
    processed_count: Arc<Mutex<u64>>,
    max_notifications: Option<u64>,
}

impl NotificationProcessor {
    fn new(max_notifications: Option<u64>) -> Self {
        Self {
            processed_count: Arc::new(Mutex::new(0)),
            max_notifications,
        }
    }
}

#[async_trait::async_trait]
impl MessageProcessor for NotificationProcessor {
    async fn process_message(&self, payload: &[u8]) -> Result<()> {
        match serde_json::from_slice::<Notification>(payload) {
            Ok(notification) => {
                let mut count = self.processed_count.lock().await;
                *count += 1;

                info!(
                    "Processed notification #{}, Type: {}, Recipient: {}, Priority: {}",
                    count,
                    notification.notification_type,
                    notification.recipient,
                    notification.priority
                );

                // Simulate notification delivery
                info!("Sending notification: {}", notification.message);
            }
            Err(e) => {
                warn!("Failed to deserialize notification: {}", e);
            }
        }
        Ok(())
    }

    fn should_continue(&self) -> bool {
        let count = futures::executor::block_on(async { *self.processed_count.lock().await });
        match self.max_notifications {
            Some(max) => count < max,
            None => true,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Advanced Iggy Microservices Example");

    // Create custom configuration with multiple streams and topics
    let config = IggyConfig {
        server: Default::default(),
        authentication: Default::default(),
        streams: vec![
            StreamConfig {
                id: 1,
                name: "user-events".to_string(),
                topics: vec![
                    TopicConfig {
                        id: 1,
                        name: "user-registrations".to_string(),
                        partitions_count: 2,
                        message_expiry: None,
                        max_topic_size: None,
                    },
                    TopicConfig {
                        id: 2,
                        name: "user-actions".to_string(),
                        partitions_count: 1,
                        message_expiry: None,
                        max_topic_size: None,
                    },
                ],
            },
            StreamConfig {
                id: 2,
                name: "order-events".to_string(),
                topics: vec![
                    TopicConfig {
                        id: 1,
                        name: "order-created".to_string(),
                        partitions_count: 1,
                        message_expiry: None,
                        max_topic_size: None,
                    },
                    TopicConfig {
                        id: 2,
                        name: "order-updates".to_string(),
                        partitions_count: 1,
                        message_expiry: None,
                        max_topic_size: None,
                    },
                ],
            },
            StreamConfig {
                id: 3,
                name: "notifications".to_string(),
                topics: vec![
                    TopicConfig {
                        id: 1,
                        name: "email-notifications".to_string(),
                        partitions_count: 1,
                        message_expiry: None,
                        max_topic_size: None,
                    },
                    TopicConfig {
                        id: 2,
                        name: "push-notifications".to_string(),
                        partitions_count: 1,
                        message_expiry: None,
                        max_topic_size: None,
                    },
                ],
            },
        ],
        client: Default::default(),
    };

    // Create client with custom configuration
    let client = create_client_with_config(config).await?;
    info!("Successfully connected to Iggy server with custom configuration");

    // Clone client for different services (it's cheap due to Arc)
    let user_client = client.clone();
    let order_client = client.clone();
    let notification_client = client.clone();

    // Run multiple producers and consumers in parallel
    let user_producer = tokio::spawn(async move {
        let producer_service = create_producer_service(user_client, 1, 1, 1).await?;
        let handler = UserEventHandler::new(Some(10));
        producer_service.run_with_handler(handler).await
    });

    let order_consumer = tokio::spawn(async move {
        let consumer_service = create_consumer_service(order_client, 2, 1, Some(1), true).await?;
        let processor = OrderEventProcessor::new(Some(5));
        consumer_service.run_with_processor(processor, 0).await
    });

    let notification_consumer = tokio::spawn(async move {
        let consumer_service =
            create_consumer_service(notification_client, 3, 1, Some(1), true).await?;
        let processor = NotificationProcessor::new(Some(8));
        consumer_service.run_with_processor(processor, 0).await
    });

    // Wait for all services to complete
    let (user_result, order_result, notification_result) =
        tokio::try_join!(user_producer, order_consumer, notification_consumer)
            .map_err(|e| IggyError::MessageProcessingError(format!("Task join error: {}", e)))?;

    user_result
        .map_err(|e| IggyError::MessageProcessingError(format!("User producer error: {}", e)))?;
    order_result
        .map_err(|e| IggyError::MessageProcessingError(format!("Order consumer error: {}", e)))?;
    notification_result.map_err(|e| {
        IggyError::MessageProcessingError(format!("Notification consumer error: {}", e))
    })?;

    info!("Advanced example completed successfully");
    Ok(())
}
