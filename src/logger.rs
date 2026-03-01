use crate::client::proto::{e_event, EEvent, TaskLogEvent, TaskProgressEvent};
use crate::client::TercenClient;
use crate::error::Result;

/// Logger for sending log messages and progress updates to Tercen
pub struct TercenLogger<'a> {
    client: &'a TercenClient,
    task_id: String,
}

impl<'a> TercenLogger<'a> {
    /// Create a new logger for a specific task
    pub fn new(client: &'a TercenClient, task_id: String) -> Self {
        TercenLogger { client, task_id }
    }

    /// Send a log message to Tercen
    pub async fn log(&self, message: impl Into<String>) -> Result<()> {
        let log_event = EEvent {
            object: Some(e_event::Object::Tasklogevent(TaskLogEvent {
                task_id: self.task_id.clone(),
                message: message.into(),
                ..Default::default()
            })),
        };

        self.send_event(log_event).await
    }

    /// Send a progress update to Tercen
    #[allow(dead_code)]
    pub async fn progress(&self, _percent: f64, message: impl Into<String>) -> Result<()> {
        let progress_event = EEvent {
            object: Some(e_event::Object::Taskprogressevent(TaskProgressEvent {
                task_id: self.task_id.clone(),
                message: message.into(),
                // Note: Progress is stored in the message field in TaskProgressEvent
                // The percent parameter is included here for API convenience
                ..Default::default()
            })),
        };

        self.send_event(progress_event).await
    }

    /// Send an event to Tercen's EventService
    async fn send_event(&self, event: EEvent) -> Result<()> {
        let mut event_service = self.client.event_service()?;
        let request = tonic::Request::new(event);

        event_service.create(request).await?;
        Ok(())
    }
}
