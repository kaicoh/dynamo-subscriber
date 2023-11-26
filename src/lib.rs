//! Wrap [Amazon DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/streamsmain.html)
//! and use it as [Rust Stream](https://docs.rs/futures-core/0.3.29/futures_core/stream/trait.Stream.html).
//!
//! ## Getting Started
//!
//! A simple example is as follows. Edit your **Cargo.toml** at first.
//!
//! ```toml
//! [dependencies]
//! dynamo-subscriber = "0.1"
//! aws-config = "1.0.1"
//! tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
//! tokio-stream = "0.1.14"
//! ```
//!
//! Then in code, assuming that the dynamodb-local instance is running on localhost:8000
//! and "People" table exists, you can subscribe dynamodb streams from "People" table with
//! the following.
//!
//! This stream emits a vector of [`Record`](aws_sdk_dynamodbstreams::types::Record).
//!
//! ```rust,no_run
//! use aws_config::BehaviorVersion;
//! use dynamo_subscriber as subscriber;
//! use tokio_stream::StreamExt;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = aws_config::load_defaults(BehaviorVersion::latest())
//!         .await
//!         .into_builder()
//!         .endpoint_url("http://localhost:8000")
//!         .build();
//!
//!     let client = subscriber::Client::new(&config);
//!     let mut stream = subscriber::stream::builder()
//!         .table_name("People")
//!         .client(client)
//!         .build();
//!
//!     while let Some(records) = stream.next().await {
//!         println!("{:#?}", records);
//!     }
//! }
//! ```
//!
//! ## AWS SDK Dependency
//!
//! To build [`Client`] of this crate, you must pass the reference for
//! [`SdkConfig`](aws_config::SdkConfig).

#[macro_use]
mod macros;

/// Client for calling AWS APIs.
pub mod client;

/// Common errors.
pub mod error;

/// Data structures used by operations.
pub mod types;

/// Implementation for Dynamodb Streams.
pub mod stream;

pub use client::{Client, DynamodbClient};
