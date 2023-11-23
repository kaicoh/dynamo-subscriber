mod stream;

use super::{types, DynamodbClient, StreamConsumer, StreamProducer};

pub use stream::{WatchStream, WatchStreamBuilder};

pub fn builder<Client>() -> WatchStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    WatchStreamBuilder::new()
}
