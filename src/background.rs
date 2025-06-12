use crate::Url;
use std::{
    error,
    fmt::{self, Debug},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{instrument::WithSubscriber, subscriber::NoSubscriber};

use crate::HoneycombEvent;

pub struct EventQueue {
    pub inflight: Vec<HoneycombEvent>,
    pub queue: Vec<HoneycombEvent>,
}

impl EventQueue {
    pub fn new() -> Self {
        Self {
            inflight: Vec::new(),
            queue: Vec::new(),
        }
    }

    pub fn push(&mut self, event: HoneycombEvent) {
        // TODO: add limit?
        self.queue.push(event);
    }

    pub fn drop_outstanding(&mut self) -> usize {
        let len = self.queue.len();
        self.queue.clear();
        len
    }

    pub fn handle_response_status(&mut self, result: Result<(), ()>) {
        match result {
            Ok(()) => self.inflight.clear(),
            Err(()) => {
                // return inflight to top of queue
                self.inflight.append(&mut self.queue);
                std::mem::swap(&mut self.inflight, &mut self.queue);
            }
        }
    }

    pub fn new_request_needed(&self) -> bool {
        !self.queue.is_empty()
    }

    pub fn prepare_request(&mut self) -> Vec<HoneycombEvent> {
        assert!(
            self.inflight.is_empty(),
            "cannot send new request when one is already inflight"
        );
        std::mem::swap(&mut self.inflight, &mut self.queue);
        self.inflight.clone()
    }
}

#[derive(Debug)]
pub struct BadRedirect {
    status: u16,
    to: Url,
}

impl fmt::Display for BadRedirect {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Following such a redirect drops the request body, and will likely
        // give an HTTP 200 response even though nobody ever looked at the POST
        // body.
        //
        // This can e.g. happen for login redirects when you post to a
        // login-protected URL.
        write!(f, "invalid HTTP {} redirect to {}", self.status, self.to)
    }
}

impl error::Error for BadRedirect {}

pub struct BackgroundTask {
    honeycomb_endpoint_url: Url,
    receiver: mpsc::Receiver<Option<HoneycombEvent>>,
    queue: EventQueue,
    http_client: reqwest::Client,
    backoff_count: u32,
    backoff: Option<Pin<Box<tokio::time::Sleep>>>,
    quitting: bool,
    send_task: Option<
        Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send + 'static>>,
    >,
}

impl BackgroundTask {
    pub fn new(
        honeycomb_endpoint_url: Url,
        http_headers: reqwest::header::HeaderMap,
        receiver: mpsc::Receiver<Option<HoneycombEvent>>,
    ) -> BackgroundTask {
        BackgroundTask {
            receiver,
            honeycomb_endpoint_url,
            queue: EventQueue::new(),
            http_client: reqwest::Client::builder()
                .user_agent(concat!(
                    env!("CARGO_PKG_NAME"),
                    "/",
                    env!("CARGO_PKG_VERSION")
                ))
                .default_headers(http_headers)
                .redirect(reqwest::redirect::Policy::custom(|a| {
                    let status = a.status().as_u16();
                    // the two redirect types that discard the body
                    if status == 302 || status == 303 {
                        let to = a.url().clone();
                        return a.error(BadRedirect { status, to });
                    }
                    // delegate to default impl
                    reqwest::redirect::Policy::default().redirect(a)
                }))
                .build()
                .expect("building TLS backend initialization and loading system configuration to succeed"),
            backoff_count: 0,
            backoff: None,
            quitting: false,
            send_task: None,
        }
    }

    /// whether the send queue should be dropped, and the backoff duration
    fn backoff_time(&self) -> (bool, Duration) {
        let backoff_time = if self.backoff_count >= 1 {
            Duration::from_millis(
                500u64
                    .checked_shl(self.backoff_count - 1)
                    .unwrap_or(u64::MAX),
            )
        } else {
            Duration::from_millis(0)
        };
        (
            backoff_time >= Duration::from_secs(30),
            std::cmp::min(backoff_time, Duration::from_secs(600)),
        )
    }
}

impl Future for BackgroundTask {
    type Output = ();

    fn poll(mut self: Pin<&mut BackgroundTask>, cx: &mut Context<'_>) -> Poll<()> {
        // prevent infinite log recursion
        let mut default_guard = tracing::subscriber::set_default(NoSubscriber::default());

        while let Poll::Ready(maybe_maybe_item) = Pin::new(&mut self.receiver).poll_recv(cx) {
            match maybe_maybe_item {
                Some(Some(item)) => self.queue.push(item),
                Some(None) => self.quitting = true, // Explicit close.
                None => self.quitting = true,       // The sender was dropped.
            }
        }

        let mut backing_off = if let Some(backoff) = &mut self.backoff {
            matches!(Pin::new(backoff).poll(cx), Poll::Pending)
        } else {
            false
        };
        if !backing_off {
            self.backoff = None;
        }
        loop {
            if let Some(send_task) = &mut self.send_task {
                match Pin::new(send_task).poll(cx) {
                    Poll::Ready(res) => {
                        if let Err(e) = &res {
                            let (drop_outstanding, backoff_time) = self.backoff_time();
                            drop(default_guard);
                            tracing::error!(
                                error_count = self.backoff_count + 1,
                                ?backoff_time,
                                error = %e,
                                "couldn't send logs to honeycomb",
                            );
                            default_guard =
                                tracing::subscriber::set_default(NoSubscriber::default());
                            if drop_outstanding {
                                let num_dropped: usize = self.queue.drop_outstanding();
                                drop(default_guard);
                                tracing::error!(
                                    num_dropped,
                                    "dropped outstanding messages due to sending errors",
                                );
                                default_guard =
                                    tracing::subscriber::set_default(NoSubscriber::default());
                            }
                            self.backoff = Some(Box::pin(tokio::time::sleep(backoff_time)));
                            self.backoff_count += 1;
                            backing_off = true;
                        } else {
                            self.backoff_count = 0;
                        }
                        self.queue.handle_response_status(res.map_err(|_| ()));
                        self.send_task = None;
                    }
                    Poll::Pending => {}
                }
            }
            if self.send_task.is_none() && !backing_off && self.queue.new_request_needed() {
                let events = self.queue.prepare_request();
                let body = serde_json::to_vec(&events)
                    .expect("none of the tracing field types can fail to serialize");

                // TODO: compress? docs mention supported compression formats
                let request_builder = self.http_client.post(self.honeycomb_endpoint_url.clone());
                self.send_task = Some(Box::pin(
                    async move {
                        request_builder
                            .header(reqwest::header::CONTENT_TYPE, "application/json")
                            .body(body)
                            .send()
                            .await?
                            .error_for_status()?;
                        Ok(())
                    }
                    .with_subscriber(NoSubscriber::default()),
                ));
            } else {
                break;
            }
        }
        if self.quitting && self.send_task.is_none() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// Handle to cleanly shut down the `BackgroundTask`.
///
/// It'll still try to send all available data and then quit.
pub struct BackgroundTaskController {
    sender: mpsc::Sender<Option<HoneycombEvent>>,
}

impl BackgroundTaskController {
    pub fn new(sender: mpsc::Sender<Option<HoneycombEvent>>) -> Self {
        Self { sender }
    }

    /// Shut down the associated `BackgroundTask`.
    pub async fn shutdown(&self) {
        // Ignore the error. If no one is listening, it already shut down.
        let _ = self.sender.send(None).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        hash::Hash,
        sync::{Arc, RwLock},
    };

    use super::*;
    use crate::{HONEYCOMB_AUTH_HEADER_NAME, background::EventQueue};
    use axum::{
        Json, Router,
        extract::Request,
        middleware::{self, Next},
        response::{IntoResponse, Response},
        routing::post,
    };
    use chrono::Utc;
    use reqwest::StatusCode;
    use serde_json::json;

    fn new_event(span_id: Option<u64>) -> HoneycombEvent {
        HoneycombEvent {
            timestamp: Utc::now(),
            span_id,
            parent_id: None,
            service_name: None,
            level: "INFO",
            name: "name".to_owned(),
            target: "target".to_owned(),
            fields: Default::default(),
        }
    }

    #[test]
    fn event_queue_on_success() {
        let mut queue = EventQueue::new();
        assert!(!queue.new_request_needed());
        let evt = new_event(Some(0));
        queue.push(evt.clone());
        queue.push(evt.clone());
        assert_eq!((queue.inflight.len(), queue.queue.len()), (0, 2));
        assert!(queue.new_request_needed());
        assert_eq!(queue.prepare_request().len(), 2);
        assert_eq!((queue.inflight.len(), queue.queue.len()), (2, 0));
        queue.push(HoneycombEvent {
            span_id: Some(1),
            ..evt.clone()
        });

        queue.handle_response_status(Ok(()));
        assert!(
            queue.inflight.is_empty(),
            "nothing inflight after response received"
        );
        assert_eq!(
            queue
                .queue
                .iter()
                .map(|i| i.span_id.unwrap())
                .collect::<Vec<_>>(),
            vec![1],
            "queue unchanged, successful events not returned to queue"
        );
    }

    #[test]
    fn event_queue_on_failure() {
        let mut queue = EventQueue::new();
        assert!(!queue.new_request_needed());
        let evt = new_event(Some(0));
        queue.push(evt.clone());
        queue.push(evt.clone());
        assert_eq!((queue.inflight.len(), queue.queue.len()), (0, 2));
        assert!(queue.new_request_needed());
        assert_eq!(queue.prepare_request().len(), 2);
        assert_eq!((queue.inflight.len(), queue.queue.len()), (2, 0));
        queue.push(HoneycombEvent {
            span_id: Some(1),
            ..evt.clone()
        });

        queue.handle_response_status(Err(()));
        assert!(
            queue.inflight.is_empty(),
            "nothing inflight after response received"
        );
        assert_eq!(queue.queue.len(), 3, "failed responses returned to queue");
        assert_eq!(
            queue
                .queue
                .iter()
                .map(|i| i.span_id.unwrap())
                .collect::<Vec<_>>(),
            vec![0, 0, 1],
            "failed requests returned to queue ahead of unsent requests"
        );
    }

    const MOCK_API_KEY: &'static str = "xxx-testing-api-key-xxx";

    type Dataset = Vec<HashMap<String, serde_json::Value>>;

    #[derive(Debug)]
    struct AppState {
        datasets: RwLock<HashMap<String, Dataset>>,
    }

    async fn middleware_auth_with_mock_key(request: Request, next: Next) -> Response {
        let api_key = match request.headers().get(HONEYCOMB_AUTH_HEADER_NAME) {
            None => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"error": "missing API key header"})),
                )
                    .into_response();
            }
            Some(api_key) => api_key,
        };
        if api_key != MOCK_API_KEY {
            return (StatusCode::FORBIDDEN, "invalid API key").into_response();
        }
        next.run(request).await
    }

    fn build_mock_server() -> Router<AppState> {
        let state = Arc::new(AppState {
            datasets: RwLock::new(HashMap::new()),
        });
        Router::new()
            .route("/1/batch/{dataset}", post(post_create_events))
            .layer(middleware::from_fn(middleware_auth_with_mock_key))
            .with_state(state.clone())
    }

    async fn post_create_events() {}
}
