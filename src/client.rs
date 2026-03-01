use crate::error::{Result, TercenError};
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Request, Status};

// Include the generated protobuf code
#[allow(
    dead_code,
    unused_imports,
    clippy::large_enum_variant,
    clippy::enum_variant_names
)]
pub mod proto {
    tonic::include_proto!("tercen");
}

use proto::document_service_client::DocumentServiceClient;
use proto::event_service_client::EventServiceClient;
use proto::file_service_client::FileServiceClient;
use proto::table_schema_service_client::TableSchemaServiceClient;
use proto::task_service_client::TaskServiceClient;
use proto::user_service_client::UserServiceClient;
use proto::workflow_service_client::WorkflowServiceClient;

/// Type alias for authenticated TaskService client
pub type AuthTaskServiceClient =
    TaskServiceClient<tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>>;

/// Type alias for authenticated UserService client
#[allow(dead_code)]
pub type AuthUserServiceClient =
    UserServiceClient<tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>>;

/// Type alias for authenticated EventService client
pub type AuthEventServiceClient =
    EventServiceClient<tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>>;

/// Type alias for authenticated TableSchemaService client
#[allow(dead_code)]
pub type AuthTableSchemaServiceClient = TableSchemaServiceClient<
    tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>,
>;

/// Type alias for authenticated WorkflowService client
#[allow(dead_code)]
pub type AuthWorkflowServiceClient = WorkflowServiceClient<
    tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>,
>;

/// Type alias for authenticated DocumentService client
#[allow(dead_code)]
pub type AuthDocumentServiceClient = DocumentServiceClient<
    tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>,
>;

/// Type alias for authenticated FileService client
pub type AuthFileServiceClient =
    FileServiceClient<tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>>;

/// Interceptor that adds Bearer token authentication to all requests
#[derive(Clone)]
pub struct AuthInterceptor {
    token: MetadataValue<tonic::metadata::Ascii>,
}

impl AuthInterceptor {
    fn new(token: String) -> Result<Self> {
        // gRPC expects token without "Bearer" prefix (unlike REST APIs)
        let token = token
            .parse()
            .map_err(|e| TercenError::Auth(format!("Invalid token format: {}", e)))?;

        Ok(AuthInterceptor { token })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.token.clone());
        Ok(request)
    }
}

/// Main Tercen gRPC client
pub struct TercenClient {
    channel: Channel,
    token: String,
}

impl TercenClient {
    /// Create a new TercenClient by connecting to the specified endpoint with a token
    pub async fn connect(endpoint: String, token: String) -> Result<Self> {
        // Configure TLS only for https:// endpoints
        let use_tls = endpoint.starts_with("https://");

        // Parse and connect to the endpoint
        let mut channel_builder = Channel::from_shared(endpoint.clone())
            .map_err(|e| TercenError::Config(format!("Invalid endpoint '{}': {}", endpoint, e)))?;

        // Add TLS config only for HTTPS
        if use_tls {
            let tls = ClientTlsConfig::new();
            channel_builder = channel_builder.tls_config(tls).map_err(|e| {
                TercenError::Config(format!("Failed to configure TLS for '{}': {}", endpoint, e))
            })?;
        }

        let channel = channel_builder.connect().await.map_err(|e| {
            TercenError::Connection(format!("Failed to connect to '{}': {}", endpoint, e))
        })?;

        Ok(TercenClient { channel, token })
    }

    /// Create a new TercenClient from environment variables
    ///
    /// Required environment variables:
    /// - `TERCEN_URI`: The Tercen server URI (e.g., https://tercen.com:5400)
    /// - `TERCEN_TOKEN`: The authentication token
    pub async fn from_env() -> Result<Self> {
        let uri = std::env::var("TERCEN_URI")
            .map_err(|_| TercenError::Config("TERCEN_URI environment variable not set".into()))?;

        let token = std::env::var("TERCEN_TOKEN")
            .map_err(|_| TercenError::Config("TERCEN_TOKEN environment variable not set".into()))?;

        Self::connect(uri, token).await
    }

    /// Get a UserService client with authentication
    #[allow(dead_code)]
    pub fn user_service(&self) -> Result<AuthUserServiceClient> {
        let interceptor = AuthInterceptor::new(self.token.clone())?;
        Ok(UserServiceClient::with_interceptor(
            self.channel.clone(),
            interceptor,
        ))
    }

    /// Get a TaskService client with authentication
    pub fn task_service(&self) -> Result<AuthTaskServiceClient> {
        let interceptor = AuthInterceptor::new(self.token.clone())?;
        Ok(TaskServiceClient::with_interceptor(
            self.channel.clone(),
            interceptor,
        ))
    }

    /// Get an EventService client with authentication
    pub fn event_service(&self) -> Result<AuthEventServiceClient> {
        let interceptor = AuthInterceptor::new(self.token.clone())?;
        Ok(EventServiceClient::with_interceptor(
            self.channel.clone(),
            interceptor,
        ))
    }

    /// Get a TableSchemaService client with authentication
    #[allow(dead_code)]
    pub fn table_service(&self) -> Result<AuthTableSchemaServiceClient> {
        let interceptor = AuthInterceptor::new(self.token.clone())?;
        Ok(TableSchemaServiceClient::with_interceptor(
            self.channel.clone(),
            interceptor,
        ))
    }

    /// Get a WorkflowService client with authentication
    pub fn workflow_service(&self) -> Result<AuthWorkflowServiceClient> {
        let interceptor = AuthInterceptor::new(self.token.clone())?;
        Ok(WorkflowServiceClient::with_interceptor(
            self.channel.clone(),
            interceptor,
        ))
    }

    /// Get a DocumentService client with authentication
    #[allow(dead_code)]
    pub fn document_service(&self) -> Result<AuthDocumentServiceClient> {
        let interceptor = AuthInterceptor::new(self.token.clone())?;
        Ok(DocumentServiceClient::with_interceptor(
            self.channel.clone(),
            interceptor,
        ))
    }

    /// Get a FileService client with authentication
    pub fn file_service(&self) -> Result<AuthFileServiceClient> {
        let interceptor = AuthInterceptor::new(self.token.clone())?;
        Ok(FileServiceClient::with_interceptor(
            self.channel.clone(),
            interceptor,
        ))
    }

    /// Get the underlying channel for use with arbitrary service clients
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Create an auth interceptor for use with arbitrary service clients
    pub fn auth_interceptor(&self) -> Result<AuthInterceptor> {
        AuthInterceptor::new(self.token.clone())
    }
}
