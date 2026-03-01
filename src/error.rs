use thiserror::Error;

/// Errors that can occur when interacting with Tercen services
#[derive(Debug, Error)]
pub enum TercenError {
    /// gRPC transport or protocol error
    #[error("gRPC error: {0}")]
    Grpc(Box<tonic::Status>),

    /// gRPC transport error
    #[error("Transport error: {0}")]
    Transport(Box<tonic::transport::Error>),

    /// Authentication error
    #[error("Authentication error: {0}")]
    Auth(String),

    /// Configuration error (missing env vars, invalid URIs, etc.)
    #[error("Configuration error: {0}")]
    Config(String),

    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),

    /// Data processing or validation error
    #[error("Data error: {0}")]
    Data(String),

    /// Generic error
    #[allow(dead_code)]
    #[error("{0}")]
    Other(String),
}

/// Type alias for Results using TercenError
pub type Result<T> = std::result::Result<T, TercenError>;

// Manual From implementations for boxed error types
impl From<tonic::Status> for TercenError {
    fn from(err: tonic::Status) -> Self {
        TercenError::Grpc(Box::new(err))
    }
}

impl From<tonic::transport::Error> for TercenError {
    fn from(err: tonic::transport::Error) -> Self {
        TercenError::Transport(Box::new(err))
    }
}
