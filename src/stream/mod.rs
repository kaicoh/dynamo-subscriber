mod consumer;
pub mod mpsc;
mod producer;
pub mod watch;

use super::{client::DynamodbClient, error::Error, types};

use consumer::StreamConsumer;
use producer::StreamProducer;
