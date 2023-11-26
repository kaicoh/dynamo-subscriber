mod common;

use dynamo_subscriber as subscriber;

use common::{pk, put_item, setup, teardown, wait_until_initialized};
use tokio_stream::StreamExt;

#[tokio::test]
async fn it_can_be_consumed_as_stream() {
    let config = setup().await;

    let table_name = config.table_name();
    let sdk_config = config.aws_sdk_config();

    let client = subscriber::Client::new(sdk_config);
    let mut stream = subscriber::stream::builder()
        .table_name(table_name)
        .client(client)
        .interval(None)
        .build();

    wait_until_initialized(&mut stream).await;

    // Test put item
    put_item("pk0", &sdk_config).await;

    // Receive dynamodb stream
    let records_opt = stream.next().await;
    assert!(records_opt.is_some());

    let records = records_opt.unwrap();
    assert_eq!(records.len(), 1);

    let record = records.get(0).unwrap();
    assert_eq!(pk(record), "pk0");

    // Stop streaming
    stream.close();

    let records_opt = stream.next().await;
    assert!(records_opt.is_none());

    teardown(sdk_config).await;
}
