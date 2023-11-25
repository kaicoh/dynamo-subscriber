mod common;

use dynamo_subscriber as subscriber;

use common::{pk, put_item, setup, teardown, wait_until_consuming_enabled};
use tokio_stream::StreamExt;

#[tokio::test]
async fn watch_stream() {
    let config = setup().await;

    let table_name = config.table_name();
    let sdk_config = config.aws_sdk_config();

    let client = subscriber::Client::new(sdk_config);
    let mut stream = subscriber::stream::watch::builder()
        .table_name(table_name)
        .client(client)
        .interval(None)
        .build();

    // Initial records is empty
    let records_opt = stream.next().await;
    assert!(records_opt.is_some());

    let records = records_opt.unwrap();
    assert!(records.is_empty());

    wait_until_consuming_enabled(&mut stream).await;

    // Test put item
    put_item("watch_stream_item", &sdk_config).await;

    // Receive dynamodb stream
    let records_opt = stream.next().await;
    assert!(records_opt.is_some());

    let records = records_opt.unwrap();
    assert_eq!(records.len(), 1);

    let record = records.get(0).unwrap();
    assert_eq!(pk(record), "watch_stream_item");

    teardown(sdk_config).await;
}

#[tokio::test]
async fn from_changes_watch_stream() {
    let config = setup().await;

    let table_name = config.table_name();
    let sdk_config = config.aws_sdk_config();

    let client = subscriber::Client::new(sdk_config);
    let mut stream = subscriber::stream::watch::builder()
        .table_name(table_name)
        .client(client)
        .interval(None)
        .from_changes();

    wait_until_consuming_enabled(&mut stream).await;

    // Test put item
    put_item("from_changes_stream_item", &sdk_config).await;

    // Receive dynamodb stream
    let records_opt = stream.next().await;
    assert!(records_opt.is_some());

    let records = records_opt.unwrap();
    assert_eq!(records.len(), 1);

    let record = records.get(0).unwrap();
    assert_eq!(pk(record), "from_changes_stream_item");

    teardown(sdk_config).await;
}

#[tokio::test]
async fn mpsc_stream() {
    let config = setup().await;

    let table_name = config.table_name();
    let sdk_config = config.aws_sdk_config();

    let client = subscriber::Client::new(sdk_config);
    let mut stream = subscriber::stream::mpsc::builder()
        .table_name(table_name)
        .client(client)
        .interval(None)
        .build();

    wait_until_consuming_enabled(&mut stream).await;

    // Test put item
    put_item("mpsc_stream_item", &sdk_config).await;

    // Receive dynamodb stream
    let records_opt = stream.next().await;
    assert!(records_opt.is_some());

    let records = records_opt.unwrap();
    assert_eq!(records.len(), 1);

    let record = records.get(0).unwrap();
    assert_eq!(pk(record), "mpsc_stream_item");

    teardown(sdk_config).await;
}
