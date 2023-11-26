use aws_config::{retry::RetryConfig, BehaviorVersion, Region, SdkConfig};
use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};
use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType, StreamSpecification, StreamViewType,
    },
    Client,
};
use aws_sdk_dynamodbstreams::types::Record;
use dynamo_subscriber::stream::StreamConsumerExt;
use tokio::time::{sleep, Duration};
use ulid::Ulid;

const TABLE: &str = "People";
const PK: &str = "Id";

pub struct TestConfig {
    table_name: String,
    config: SdkConfig,
}

impl TestConfig {
    pub fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    pub fn aws_sdk_config(&self) -> &SdkConfig {
        &self.config
    }
}

pub async fn setup() -> TestConfig {
    let creds = Credentials::from_keys(Ulid::new(), Ulid::new(), None);
    let creds_provider = SharedCredentialsProvider::new(creds);

    let retry = RetryConfig::standard().with_max_attempts(5);

    let config = SdkConfig::builder()
        .endpoint_url("http://localhost:8000")
        .credentials_provider(creds_provider)
        .retry_config(retry)
        .behavior_version(BehaviorVersion::latest())
        .region(Some(Region::from_static("us-east-1")))
        .build();

    create_table(&config).await;

    TestConfig {
        table_name: TABLE.to_string(),
        config,
    }
}

pub async fn put_item(pk: &str, config: &SdkConfig) {
    Client::new(config)
        .put_item()
        .table_name(TABLE)
        .item(PK, AttributeValue::S(pk.into()))
        .send()
        .await
        .unwrap();
}

pub async fn teardown(config: &SdkConfig) {
    drop_table(config).await;
}

pub async fn wait_until_consuming_enabled(stream: &mut impl StreamConsumerExt) {
    let mut res = stream.initialized();

    while !res {
        sleep(Duration::from_millis(100)).await;
        res = stream.initialized();
    }
}

pub fn pk(record: &Record) -> String {
    record
        .dynamodb()
        .and_then(|dynamodb| dynamodb.keys())
        .unwrap()
        .get(PK)
        .unwrap()
        .as_s()
        .unwrap()
        .to_string()
}

async fn create_table(config: &SdkConfig) {
    Client::new(config)
        .create_table()
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(PK)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .table_name(TABLE)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(PK)
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewImage)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

async fn drop_table(config: &SdkConfig) {
    Client::new(config)
        .delete_table()
        .table_name(TABLE)
        .send()
        .await
        .unwrap();
}
