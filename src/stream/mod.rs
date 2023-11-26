mod channel;
mod consumer;
pub mod mpsc;
mod producer;
pub mod watch;

use super::{client::DynamodbClient, error::Error, types};

pub use consumer::StreamConsumerExt;
pub use producer::StreamProducerExt;
