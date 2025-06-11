use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{
    collections::HashMap,
    error,
    fmt::{self, Debug},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tracing::{
    Subscriber,
    field::{Field, Visit},
    instrument::WithSubscriber,
    span,
    subscriber::NoSubscriber,
};
use tracing_subscriber::registry::LookupSpan;

pub mod builder;
pub use builder::{Builder, builder};
pub use reqwest::Url;

fn event_channel(
    size: usize,
) -> (
    mpsc::Sender<Option<HoneycombEvent>>,
    mpsc::Receiver<Option<HoneycombEvent>>,
) {
    mpsc::channel(16384) // make it so big that if we drop events, it's already kind of bad
}

// list of reserved field names (case insensitive):
// trace.span_id
// trace.trace_id
// trace.parent_id
// service.name
// level
// Timestamp
// name
// target
// duration_ms

#[derive(Serialize, Clone)]
struct HoneycombEvent {
    #[serde(serialize_with = "serialize_datetime_as_rfc3339")]
    timestamp: DateTime<Utc>,

    #[serde(rename = "trace.span_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    span_id: Option<u64>,

    #[serde(rename = "trace.parent_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<u64>,

    #[serde(rename = "service.name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    service_name: Option<String>,
    level: &'static str,
    name: String,
    target: String,
    // TODO: custom value type
    #[serde(flatten)]
    fields: Fields,
}

fn serialize_datetime_as_rfc3339<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
}

pub struct Error(ErrorKind);

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl error::Error for Error {}

#[derive(Debug)]
pub enum ErrorKind {
    InvalidHoneycombUrl,
}

pub struct Layer {
    // TODO: custom value type
    extra_fields: HashMap<String, serde_json::Value>,
    service_name: Option<String>,
    sender: mpsc::Sender<Option<HoneycombEvent>>,
}

#[derive(Clone, Default, Serialize)]
struct Fields {
    #[serde(flatten)]
    fields: HashMap<String, serde_json::Value>,
}

impl Fields {
    fn new(fields: HashMap<String, serde_json::Value>) -> Self {
        Self { fields }
    }

    fn record<T: Into<serde_json::Value>>(&mut self, field: &Field, value: T) {
        self.fields.insert(field.name().into(), value.into());
    }
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record(field, format!("{:?}", value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record(field, value);
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record(field, value);
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record(field, value);
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record(field, value);
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value);
    }
    fn record_error(&mut self, field: &Field, value: &(dyn error::Error + 'static)) {
        self.record(field, format!("{}", value));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> tracing_subscriber::Layer<S> for Layer {
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx
            .span(id)
            .expect("span passed to on_new_span is open, valid, and stored by subscriber");
        let mut extensions = span.extensions_mut();
        if extensions.get_mut::<Fields>().is_some() {
            return;
        }
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        extensions.insert(fields);
    }

    fn on_record(
        &self,
        id: &span::Id,
        values: &span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx
            .span(id)
            .expect("span passed to on_new_span is open, valid, and stored by subscriber");
        let mut extensions = span.extensions_mut();
        let fields = extensions
            .get_mut::<Fields>()
            .expect("fields extension was inserted by on_new_span");
        values.record(fields);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let timestamp = Utc::now();
        // removed: tracing-log support by calling .normalized_metadata()
        let meta = event.metadata();
        let mut fields = Fields::new(self.extra_fields.clone());
        let span_id = ctx.current_span().id().cloned();
        // parent will be None if event is child of current span
        let leaf_span_id = event.parent().or_else(|| span_id.as_ref());
        let parent_id = leaf_span_id.as_ref().and_then(|id| {
            for span in ctx.span_scope(id)?.from_root() {
                fields.fields.extend(
                    span.extensions()
                        .get::<Fields>()
                        .expect("span registered in on_new_span")
                        .fields
                        .iter()
                        .map(|(field, val)| (field.clone(), val.clone())),
                );
            }

            // first is current, second is parent
            ctx.span_scope(&id)
                .expect("lookup of same ID already suceeded")
                .nth(1)
                .map(|span| span.id())
        });
        event.record(&mut fields);
        // don't care if channel closed. if capacity is reached, we have larger problems
        let _ = self.sender.try_send(Some(HoneycombEvent {
            timestamp,
            span_id: span_id.map(|id| id.into_u64()),
            parent_id: parent_id.map(|id| id.into_u64()),
            service_name: self.service_name.clone(),
            level: meta.level().as_str(),
            name: meta.name().to_owned(),
            target: meta.target().to_owned(),
            fields,
        }));
    }

    // exit can happen many times for same span, close happens only once
    fn on_exit(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let timestamp = Utc::now();
        let span = ctx
            .span(id)
            .expect("span passed to on_new_span is open, valid, and stored by subscriber");
        let meta = span.metadata();
        let mut fields = Fields::new(self.extra_fields.clone());
        let parent_id = span.parent().map(|p| p.id());
        if let Some(scope_iter) = parent_id.as_ref().and_then(|p_id| ctx.span_scope(p_id)) {
            for span in scope_iter.from_root() {
                fields.fields.extend(
                    span.extensions()
                        .get::<Fields>()
                        .expect("span registered in on_new_span")
                        .fields
                        .iter()
                        .map(|(field, val)| (field.clone(), val.clone())),
                );
            }
        }
        fields.fields.extend(
            span.extensions()
                .get::<Fields>()
                .expect("fields extension was inserted by on_new_span")
                .fields
                .clone(),
        );

        let _ = self.sender.try_send(Some(HoneycombEvent {
            timestamp,
            span_id: Some(id.into_u64()),
            parent_id: parent_id.map(|id| id.into_u64()),
            service_name: self.service_name.clone(),
            level: meta.level().as_str(),
            name: meta.name().to_owned(),
            target: meta.target().to_owned(),
            fields,
        }));
    }
}

struct EventQueue {
    inflight: Vec<HoneycombEvent>,
    queue: Vec<HoneycombEvent>,
}

impl EventQueue {
    fn new() -> Self {
        Self {
            inflight: Vec::new(),
            queue: Vec::new(),
        }
    }

    fn push(&mut self, event: HoneycombEvent) {
        // TODO: add limit?
        self.queue.push(event);
    }

    fn drop_outstanding(&mut self) -> usize {
        let len = self.queue.len();
        self.queue.clear();
        len
    }

    fn handle_response_status(&mut self, result: Result<(), ()>) {
        match result {
            Ok(()) => self.inflight.clear(),
            Err(()) => {
                // return inflight to top of queue
                self.inflight.append(&mut self.queue);
                std::mem::swap(&mut self.inflight, &mut self.queue);
            }
        }
    }

    fn new_request_needed(&self) -> bool {
        !self.queue.is_empty()
    }

    fn prepare_request(&mut self) -> Vec<HoneycombEvent> {
        assert!(
            self.inflight.is_empty(),
            "cannot send new request when one is already inflight"
        );
        std::mem::swap(&mut self.inflight, &mut self.queue);
        self.inflight.clone()
    }
}

#[derive(Debug)]
struct BadRedirect {
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
    receiver: ReceiverStream<Option<HoneycombEvent>>,
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
    fn new(
        honeycomb_endpoint_url: Url,
        http_headers: reqwest::header::HeaderMap,
        receiver: mpsc::Receiver<Option<HoneycombEvent>>,
    ) -> Result<BackgroundTask, Error> {
        Ok(BackgroundTask {
            receiver: ReceiverStream::new(receiver),
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
                .expect("reqwest client builder"),
            backoff_count: 0,
            backoff: None,
            quitting: false,
            send_task: None,
        })
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

        while let Poll::Ready(maybe_maybe_item) = Pin::new(&mut self.receiver).poll_next(cx) {
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
                                "couldn't send logs to loki",
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
    /// Shut down the associated `BackgroundTask`.
    pub async fn shutdown(&self) {
        // Ignore the error. If no one is listening, it already shut down.
        let _ = self.sender.send(None).await;
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    const OTEL_FIELD_SPAN_ID: &'static str = "trace.span_id";
    // const OTEL_FIELD_TRACE_ID: &'static str = "trace.trace_id";
    const OTEL_FIELD_PARENT_ID: &'static str = "trace.parent_id";
    const OTEL_FIELD_SERVICE_NAME: &'static str = "service.name";
    const OTEL_FIELD_LEVEL: &'static str = "level";
    const OTEL_FIELD_TIMESTAMP: &'static str = "timestamp";
    const OTEL_FIELD_DURATION_MS: &'static str = "duration_ms";

    fn check_ev_map_depth_one(ev_map: &serde_json::Map<String, Value>) {
        for (key, val) in ev_map.iter() {
            assert!(
                !matches!(val, Value::Object(_)),
                "event is not depth one: key {:#?} = {:#?}",
                key,
                val
            );
        }
    }

    #[test]
    fn tracing_layer() {
        let (sender, mut receiver) = event_channel();
        let mut layer = Layer {
            extra_fields: Default::default(),
            service_name: Some("service_name".to_owned()),
            sender,
        };
        layer
            .extra_fields
            .insert("my_extra_field".to_owned(), json!("extra_field_val"));
        let subscriber = tracing_subscriber::registry().with(layer);

        let (or_val_gp, or_val_p, or_val_c, or_val_e) = (0, 1, 2, 3);
        let (gp_val, p_val, c_val, e_val) = (40, 41, 42, 43);

        let before = Utc::now();
        let (grandparent_id, parent_id, child_id) =
            tracing::subscriber::with_default(subscriber, || {
                let grandparent_span = tracing::span!(
                    Level::DEBUG,
                    "grandparent span",
                    overridden_field = or_val_gp,
                    grandparent_field = gp_val
                );
                let _gp_enter = grandparent_span.enter();

                let parent_span = tracing::span!(
                    Level::DEBUG,
                    "parent span",
                    overridden_field = or_val_p,
                    parent_field = p_val
                );
                let _p_enter = parent_span.enter();

                let child_span = tracing::span!(
                    Level::TRACE,
                    "child span",
                    overridden_field = or_val_c,
                    child_field = c_val
                );
                let _enter = child_span.enter();

                tracing::event!(
                    Level::INFO,
                    overridden_field = or_val_e,
                    event_field = e_val,
                    "my event"
                );
                (
                    grandparent_span.id().unwrap(),
                    parent_span.id().unwrap(),
                    child_span.id().unwrap(),
                )
            });
        let after = Utc::now();

        let num_events = receiver.len();
        assert_eq!(
            num_events,
            4,
            "expected 4 events after test, got {}",
            receiver.len()
        );
        let mut events = Vec::with_capacity(num_events);
        assert_eq!(
            receiver.blocking_recv_many(&mut events, num_events),
            num_events,
            "expected to receive all events in one go"
        );
        let events = events.into_iter().map(|i| i.unwrap()).collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .map(|evt| (evt.parent_id, evt.span_id.unwrap(),))
                .collect::<Vec<_>>(),
            vec![
                (Some(parent_id.into_u64()), child_id.into_u64(),), // the event
                (Some(parent_id.into_u64()), child_id.into_u64(),), // child_span closing
                (Some(grandparent_id.into_u64()), parent_id.into_u64(),), // parent_span closing
                (None, grandparent_id.into_u64())                   // grandparent_span closing
            ]
        );
        assert_eq!(
            events
                .iter()
                .map(|evt| (evt.fields.fields.get("overridden_field")))
                .collect::<Vec<_>>(),
            vec![
                (Some(&json!(or_val_e))),  // the event
                (Some(&json!(or_val_c))),  // child_span closing
                (Some(&json!(or_val_p))),  // parent_span closing
                (Some(&json!(or_val_gp)))  // grandparent_span closing
            ]
        );

        let log_event = events.get(0);
        let ev_map = match serde_json::to_value(&log_event).unwrap() {
            Value::Object(obj) => obj,
            val => panic!(
                "expected event to serialize into map, instead got {:#?}",
                val
            ),
        };
        check_ev_map_depth_one(&ev_map);

        assert_eq!(
            ev_map.get(OTEL_FIELD_SPAN_ID),
            Some(&json!(child_id.into_u64()))
        );
        assert_eq!(
            ev_map.get(OTEL_FIELD_PARENT_ID),
            Some(&json!(parent_id.into_u64()))
        );
        assert_eq!(
            ev_map.get(OTEL_FIELD_SERVICE_NAME),
            Some(&json!("service_name"))
        );
        assert_eq!(ev_map.get(OTEL_FIELD_LEVEL), Some(&json!("INFO")));
        assert!(
            match ev_map.get(OTEL_FIELD_TIMESTAMP) {
                Some(Value::String(s)) => Some(s),
                _ => None,
            }
            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
            .and_then(|dt| {
                let dt = dt.with_timezone(&Utc);
                if before <= dt && dt <= after {
                    Some(())
                } else {
                    None
                }
            })
            .is_some(),
            "invalid timestamp: {:#?}",
            ev_map.get(OTEL_FIELD_TIMESTAMP)
        );
        // "name" field is based on line number so cannot be easily checked
        assert_eq!(
            ev_map.get("target"),
            Some(&json!(format!("{}::tests", env!("CARGO_CRATE_NAME"))))
        );

        assert_eq!(ev_map.get("event_field"), Some(&json!(e_val)));
        assert_eq!(ev_map.get("child_field"), Some(&json!(c_val)));
        assert_eq!(ev_map.get("parent_field"), Some(&json!(p_val)));
        assert_eq!(ev_map.get("grandparent_field"), Some(&json!(gp_val)));
        assert_eq!(ev_map.get("overridden_field"), Some(&json!(or_val_e)));

        let child_closing_event = events.get(1).unwrap();
        assert_eq!(
            child_closing_event.fields.fields.get("child_field"),
            Some(&json!(42))
        );

        let parent_closing_event = events.get(2).unwrap();
        let ev_map = match serde_json::to_value(&parent_closing_event).unwrap() {
            Value::Object(obj) => obj,
            val => panic!(
                "expected event to serialize into map, instead got {:#?}",
                val
            ),
        };
        check_ev_map_depth_one(&ev_map);

        assert_eq!(
            ev_map.get(OTEL_FIELD_SPAN_ID),
            Some(&json!(parent_id.into_u64()))
        );
        assert_eq!(
            ev_map.get(OTEL_FIELD_PARENT_ID),
            Some(&json!(grandparent_id.into_u64()))
        );
        assert_eq!(ev_map.get("event_field"), None);
        assert_eq!(ev_map.get("child_field"), None);
        assert_eq!(ev_map.get("parent_field"), Some(&json!(p_val)));
        assert_eq!(ev_map.get("grandparent_field"), Some(&json!(gp_val)));
        assert_eq!(ev_map.get("overridden_field"), Some(&json!(or_val_p)));
    }

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

    // #[test]
    fn temp() {
        let (sender, _receiver) = event_channel();
        let mut layer = Layer {
            extra_fields: Default::default(),
            service_name: Some("service_name".to_owned()),
            sender,
        };
        layer
            .extra_fields
            .insert("my_extra_field".to_owned(), json!("extra_field_val"));
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::span!(Level::DEBUG, "span_name", span_field = 42);
            let _enter = span.enter();
            span.record("recorded_field", 43);
        });
    }
}
