mod stream;

use super::{channel, types, DynamodbClient, StreamConsumerExt, StreamProducerExt};

pub use stream::{WatchStream, WatchStreamBuilder};

pub fn builder<Client>() -> WatchStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    WatchStreamBuilder::new()
}
