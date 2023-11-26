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
