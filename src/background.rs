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

    // TODO: in order to implement bounded memory usage by conditionally polling channel,
    // remove EventQueue and rewrite this function as a state machine
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
    use super::*;
    use crate::{
        HONEYCOMB_AUTH_HEADER_NAME, HoneycombEventInner,
        background::EventQueue,
        layer::tests::{OTEL_FIELD_LEVEL, OTEL_FIELD_PARENT_ID, OTEL_FIELD_SPAN_ID},
    };
    use axum::{
        Json, Router,
        extract::{Path, Request, State},
        http::header::HeaderMap,
        middleware::{self, Next},
        response::{IntoResponse, Response},
        routing::post,
    };
    use chrono::Utc;
    use reqwest::StatusCode;
    use serde::Deserialize;
    use serde_json::json;
    use std::{
        collections::{HashMap, hash_map::Entry},
        sync::{Arc, RwLock},
    };
    use tracing::Level;
    use tracing_mock::expect;
    use tracing_subscriber::{Layer, filter, layer::SubscriberExt};

    fn new_event(span_id: Option<u64>) -> HoneycombEvent {
        HoneycombEvent {
            time: Utc::now(),
            data: HoneycombEventInner {
                span_id,
                parent_id: None,
                service_name: None,
                level: "INFO",
                name: "name".to_owned(),
                target: "target".to_owned(),
                fields: Default::default(),
            },
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
            time: evt.time,
            data: HoneycombEventInner {
                span_id: Some(1),
                ..evt.data.clone()
            },
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
                .map(|i| i.data.span_id.unwrap())
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
            time: evt.time,
            data: HoneycombEventInner {
                span_id: Some(1),
                ..evt.data.clone()
            },
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
                .map(|i| i.data.span_id.unwrap())
                .collect::<Vec<_>>(),
            vec![0, 0, 1],
            "failed requests returned to queue ahead of unsent requests"
        );
    }

    const MOCK_API_KEY: &'static str = "xxx-testing-api-key-xxx";
    const TESTING_HEADER_NAME: &'static str = "x-tested-header";
    const TESTING_HEADER_VALUE: &'static str = "tested-header-value";
    const TEST_EXTRA_FIELD_NAME: &'static str = "test.extra_field";
    const TEST_EXTRA_FIELD_VALUE: &'static str = "extra_field_val";
    const TESTING_DATASET: &'static str = "testdataset";

    type ApiEvent = HashMap<String, serde_json::Value>;
    type Dataset = Vec<ApiEvent>;

    #[derive(Clone, Debug, Deserialize)]
    struct WrappedEvent {
        data: ApiEvent,
    }
    type DatasetPayload = Vec<WrappedEvent>;

    #[derive(Debug, Clone)]
    struct AppState {
        datasets: Arc<RwLock<HashMap<String, Dataset>>>,
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

    async fn post_create_events(
        State(state): State<AppState>,
        headers: HeaderMap,
        Path(dataset): Path<String>,
        Json(payload): Json<DatasetPayload>,
    ) -> Response {
        assert_eq!(
            headers
                .get(TESTING_HEADER_NAME)
                .and_then(|v| v.to_str().ok()),
            Some(TESTING_HEADER_VALUE),
            "missing or incorrect extra HTTP header"
        );

        if headers.get("x-induce-error").is_some() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "induced error"})),
            )
                .into_response();
        }

        payload.iter().for_each(|evt| {
            evt.data.iter().for_each(|(k, v)| {
                assert!(
                    !matches!(
                        v,
                        serde_json::Value::Array(_) | serde_json::Value::Object(_)
                    ),
                    "event must be depth 1: {} = {}",
                    k,
                    v
                )
            })
        });

        match state.datasets.write().unwrap().entry(dataset.to_string()) {
            Entry::Vacant(e) => drop(e.insert(payload.into_iter().map(|e| e.data).collect())),
            Entry::Occupied(mut e) => e.get_mut().extend(payload.into_iter().map(|e| e.data)),
        };

        Json(json!({"status": 200})).into_response()
    }

    fn build_mock_server() -> (AppState, Router<()>) {
        let state = AppState {
            datasets: Arc::new(RwLock::new(HashMap::new())),
        };
        (
            state.clone(),
            Router::new()
                .route("/1/batch/{dataset}", post(post_create_events))
                .layer(middleware::from_fn(middleware_auth_with_mock_key))
                .with_state(state),
        )
    }

    async fn run_mock_server() -> (String, AppState, impl Future<Output = ()>) {
        let (state, app) = build_mock_server();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_host = format!("http://{}", listener.local_addr().unwrap());
        let serve_task = async { axum::serve(listener, app).await.unwrap() };
        (api_host, state, serve_task)
    }

    #[tokio::test]
    async fn bg_task_simple() {
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(tracing_subscriber::fmt::layer()),
        );
        let (api_host, state, serve_task) = run_mock_server().await;
        let (layer, bg_task, bg_task_controller) = crate::builder(MOCK_API_KEY)
            .extra_field(
                TEST_EXTRA_FIELD_NAME.to_string(),
                json!(TEST_EXTRA_FIELD_VALUE),
            )
            .service_name("test-service-name".to_string())
            .http_header(TESTING_HEADER_NAME, TESTING_HEADER_VALUE)
            .unwrap()
            .build(&api_host, TESTING_DATASET)
            .unwrap();

        let _server_join_handle = tokio::spawn(serve_task.with_current_subscriber());
        let bg_join_handle = tokio::spawn(bg_task.with_current_subscriber());

        let subscriber = tracing_subscriber::registry()
            .with(layer)
            .with(tracing_subscriber::fmt::layer());
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::span!(target: "span-test-target", tracing::Level::WARN, "span name", span.field = 40);
            let _enter = span.enter();
            tracing::event!(name: "test-name", target: "test-target", tracing::Level::INFO, event.field = 41, "event message");
        });
        // flush all of the events
        bg_task_controller.shutdown().await;
        bg_join_handle.await.unwrap();

        // get an exclusive copy so that we don't have to go through the lock all the time
        let datasets = state.datasets.read().unwrap().clone();
        assert_eq!(datasets.len(), 1, "expected single dataset");
        let test_dataset = datasets
            .get(TESTING_DATASET)
            .expect("single dataset to be testing dataset");
        assert_eq!(
            test_dataset.len(),
            2,
            "expected exactly one event and one span close"
        );
        let log_event = &test_dataset[0];
        assert_eq!(log_event.get("event.field"), Some(&json!(41)));
        assert_eq!(log_event.get("span.field"), Some(&json!(40)));
        assert_eq!(log_event.get(OTEL_FIELD_LEVEL), Some(&json!("INFO")));
        assert_eq!(log_event.get("target"), Some(&json!("test-target")));
        assert_eq!(log_event.get("name"), Some(&json!("test-name")));

        let span_event = &test_dataset[1];
        assert_eq!(span_event.get("event.field"), None);
        assert_eq!(span_event.get("span.field"), Some(&json!(40)));
        assert_eq!(span_event.get(OTEL_FIELD_LEVEL), Some(&json!("WARN")));
        assert_eq!(span_event.get("target"), Some(&json!("span-test-target")));
        assert_eq!(span_event.get("name"), Some(&json!("span name")));
        assert!(span_event.get(OTEL_FIELD_SPAN_ID).is_some());
        assert_eq!(
            log_event.get(OTEL_FIELD_SPAN_ID),
            span_event.get(OTEL_FIELD_SPAN_ID)
        );
    }

    #[tokio::test]
    async fn bg_error_handle() {
        let (mock_layer, handle) = tracing_mock::layer::mock()
            .event(
                expect::event()
                    .at_level(Level::ERROR)
                    .with_fields(expect::msg("couldn't send logs to honeycomb"))
                    .with_fields(expect::field("error")), // the underlying reqwest error
            )
            .run_with_handle();
        let crate_name = module_path!().split("::").next().unwrap();
        let filter = filter::Targets::new()
            .with_target(format!("{}::background", crate_name), Level::INFO)
            .with_default(filter::LevelFilter::OFF);
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .with(mock_layer.with_filter(filter)),
        );

        let (api_host, _state, serve_task) = run_mock_server().await;
        let (honey_layer, bg_task, bg_task_controller) = crate::builder(MOCK_API_KEY)
            .extra_field(
                TEST_EXTRA_FIELD_NAME.to_string(),
                json!(TEST_EXTRA_FIELD_VALUE),
            )
            .service_name("test-service-name".to_string())
            .http_header(TESTING_HEADER_NAME, TESTING_HEADER_VALUE)
            .unwrap()
            .http_header("x-induce-error", "1")
            .unwrap()
            .build(&api_host, TESTING_DATASET)
            .unwrap();

        let _server_join_handle = tokio::spawn(serve_task.with_current_subscriber());
        let honey_bg_join_handle = tokio::spawn(bg_task.with_current_subscriber());

        let subscriber = tracing_subscriber::registry()
            .with(honey_layer)
            .with(tracing_subscriber::fmt::layer());
        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(Level::INFO, "event message");
        });
        // flush all of the events
        bg_task_controller.shutdown().await;
        honey_bg_join_handle.await.unwrap();
        handle.assert_finished();
    }
}
