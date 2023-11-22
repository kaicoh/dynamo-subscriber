use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("not found dynamodb stream from table: {0}")]
    NotFoundStream(String),
    #[error("not found dynamodb stream description from arn: {0}")]
    NotFoundStreamDescription(String),
    #[error("aws-sdk error: {0}")]
    SdkError(Box<dyn std::error::Error + Send + Sync + 'static>),
}
