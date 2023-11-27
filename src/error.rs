use thiserror::Error as ThisError;

/// All possible error types for this crate.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, ThisError)]
pub enum Error {
    /// Not found stream arn when attempting to get table description for given table name.
    #[error("not found dynamodb stream from table: {0}")]
    NotFoundStream(String),
    /// Not found stream description when attempting to get stream description.
    #[error("not found dynamodb stream description from arn: {0}")]
    NotFoundStreamDescription(String),
    /// Any other errors from the aws sdk.
    #[error("aws-sdk error: {0}")]
    SdkError(Box<dyn std::error::Error + Send + Sync + 'static>),
}
