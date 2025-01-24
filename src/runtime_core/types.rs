use std::sync::Arc;
use tokio::sync::oneshot;

pub enum CommandType {
    RequestVote,
    AppendEntries
}

pub struct Command {
    pub command_type: CommandType
}

pub struct RuntimeTaskResponse {

}

pub struct RuntimeTask {
    pub command: Arc<Command>,
    pub callback: oneshot::Sender<RuntimeTaskResponse>
}