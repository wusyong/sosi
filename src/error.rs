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
    #[error("invalid high_pri_pool_ratio. Must in range of (0.0..1.0)")]
    InvalidRatio,
    #[error("the cache cannot be sharded into too many pieces. Maximum is 20.")]
    TooManyShards,
}
