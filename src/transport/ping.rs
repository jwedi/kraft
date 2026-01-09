use tonic::{transport::Server, Request, Response, Status};
use ping::ping_pong_server::{PingPong, PingPongServer};
use ping::{PingRequest, PingResponse};
use log::{info, warn};
use crate::service_utils::errors::ServiceError;
use futures::FutureExt;
use tracing::{instrument, Instrument, Level, Span};

pub mod ping {
    tonic::include_proto!("ping"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct PingServer {}

#[tonic::async_trait]
impl PingPong for PingServer {
    async fn ping(
        &self,
        request: Request<PingRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<PingResponse>, Status> { // Return an instance of type HelloReply
        tracing::info!("received request");

        let reply = PingResponse {
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }
}