use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    //#[error("data store disconnected")]
    //Disconnect(#[from] io::Error),
    //#[error("the data for key `{0}` is not available")]
    //Redaction(String),
    //#[error("invalid header (expected {expected:?}, found {found:?})")]
    //InvalidHeader {
    //    expected: String,
    //    found: String,
    //},
    #[error("invalid block creation.")]
    InvalidBlock,
    #[error("Inserted Key should be greater than previous key.")]
    InvalidKey,
    #[error("the cache cannot be sharded into too many pieces. Maximum is 20.")]
    TooManyShards,
    #[error("Fail to perform varint conversion on {0}")]
    VarintConversion(&'static str),
    #[error(transparent)]
    FmtError(#[from] std::io::Error),
}

#[derive(Debug)]
pub enum Severity {
    None,
    SoftError,
    HardError,
    FatalError,
    UnrecoverableError,
}

#[derive(Error, Debug)]
pub enum StatusError {
    #[error("Not Found: {1}")]
    NotFound(Severity, String),
    #[error("Busy: {1}")]
    Busy(Severity, String),
    #[error("Expired: {1}")]
    Expired(Severity, String),
    #[error("Try Again: {1}")]
    TryAgain(Severity, String),
}
