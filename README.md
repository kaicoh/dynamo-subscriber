# dynamo-subscriber

Subscribe [Dynamodb Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/streamsmain.html) as [tokio-stream](https://tokio.rs/tokio/tutorial/streams).

## Overview

This library is a wrapper of Dynamodb Streams and enables using it as a tokio-stream. If you want to know what tokio-stream is and how to use it, see [this doc](https://crates.io/crates/tokio-stream).

## Example

A simple example of [watch stream](https://docs.rs/tokio-stream/0.1.14/tokio_stream/wrappers/struct.WatchStream.html). This stream detects new records are sent and pulls them automatically.

```toml
[dependencies]
dynamo-subscriber = "0.1.0"

aws-config = { version = "1.0.1" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tokio-stream = "0.1.14"
```

```rust
use aws_config::BehaviorVersion;
use dynamo_subscriber as subscriber;
use tokio_stream::StreamExt;

// This example assumes that the dynamodb-local instance is running on localhost:8000
// and "People" table exists.

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest())
        .await
        .into_builder()
        .endpoint_url("http://localhost:8000")
        .build();

    let client = subscriber::Client::new(&config);
    let mut stream = subscriber::stream::builder()
        .table_name("People")
        .client(client)
        .build();

    while let Some(records) = stream.next().await {
        println!("{:#?}", records);
    }
}
```

## License

This project is licensed under the [MIT license](LICENSE).
