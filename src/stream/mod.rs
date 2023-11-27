//! # DynamodbStream
//!
//! [`DynamodbStream`](crate::stream::DynamodbStream) represents Amazon DynamoDB Streams and you can receive records from it by
//! using it as a [tokio stream](https://docs.rs/tokio-stream/0.1.14/tokio_stream/index.html).
//!
//! ```rust,no_run
//! # use aws_config::BehaviorVersion;
//! use dynamo_subscriber as subscriber;
//! use tokio_stream::StreamExt;
//!
//! # async fn wrapper() {
//! # let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
//! # let client = subscriber::Client::new(&config);
//! // Create a stream from builder.
//! let mut stream = subscriber::stream::builder()
//!     .client(client)
//!     .table_name("People")
//!     .build();
//!
//! // Subscribe the stream to receive DynamoDB Streams records.
//! while let Some(records) = stream.next().await {
//!     println!("{:#?}", records);
//! }
//! # }
//! ```
//!
//! ## Stop polling the DynamoDB table
//!
//! Once you construct a stream, it polls the DynamoDB table and receives records permanently.
//! If you want to stop polling, extract a communication channel to the polling half of the
//! stream, and then send `Stop polling` event from the channel.
//!
//! ```rust,no_run
//! # use aws_config::BehaviorVersion;
//! use dynamo_subscriber as subscriber;
//! use tokio_stream::StreamExt;
//!
//! # async fn wrapper() {
//! # let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
//! # let client = subscriber::Client::new(&config);
//! // Create a stream from builder.
//! let mut stream = subscriber::stream::builder()
//!     .client(client)
//!     .table_name("People")
//!     .build();
//!
//! // Extract a communication channel from the stream.
//! let mut channel = stream.take_channel().unwrap();
//!
//! // Receive records from the stream.
//! let records = stream.next().await;
//! println!("{:#?}", records);
//!
//! // Send `Stop polling` event from the communication channel.
//! channel.close(|| {});
//!
//! // Now the stream is closed and no more records can be sent to the stream.
//! let records = stream.next().await;
//! assert!(records.is_none());
//! # }
//! ```

mod channel;
mod dynamodb;

use super::{client::DynamodbClient, error::Error, types};

pub use channel::ConsumerChannel;
pub use dynamodb::{DynamodbStream, DynamodbStreamBuilder};

/// Create [`DynamodbStreamBuilder`].
pub fn builder<C: DynamodbClient + 'static>() -> DynamodbStreamBuilder<C> {
    DynamodbStreamBuilder::new()
}
