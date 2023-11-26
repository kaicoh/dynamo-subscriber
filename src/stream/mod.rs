mod channel;
mod dynamodb;

use super::{client::DynamodbClient, error::Error, types};

pub use dynamodb::{DynamodbStream, DynamodbStreamBuilder};

pub fn builder<C: DynamodbClient + 'static>() -> DynamodbStreamBuilder<C> {
    DynamodbStreamBuilder::new()
}
