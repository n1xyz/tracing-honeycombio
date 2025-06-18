use chrono::Utc;
use std::{collections::HashMap, time::Instant};
use tokio::sync::mpsc;
use tracing::{Level, Subscriber, span};
use tracing_subscriber::registry::LookupSpan;

use crate::{Fields, HoneycombEvent, HoneycombEventInner, TraceId};

fn level_as_honeycomb_str(level: &Level) -> &'static str {
    match *level {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    }
}

struct Timings {
    start: Instant,
    idle: u64,
    busy: u64,
    last: Instant,
    entered_depth: u64,
}

impl Timings {
    fn new() -> Self {
        let start = Instant::now();
        Self {
            start,
            idle: 0,
            busy: 0,
            last: start,
            entered_depth: 0,
        }
    }
}

pub struct Layer {
    // TODO: custom value type
    extra_fields: HashMap<String, serde_json::Value>,
    service_name: Option<String>,
    sender: mpsc::Sender<Option<HoneycombEvent>>,
}

impl Layer {
    pub fn new(
        extra_fields: HashMap<String, serde_json::Value>,
        service_name: Option<String>,
        sender: mpsc::Sender<Option<HoneycombEvent>>,
    ) -> Self {
        Self {
            extra_fields,
            service_name,
            sender,
        }
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
        debug_assert!(
            extensions.get_mut::<Fields>().is_none(),
            "on_new_span should only ever be called once, something is buggy"
        );
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        extensions.insert(fields);
        let timings = Timings::new();
        extensions.insert(timings);
        let trace_id = span
            .parent()
            .and_then(|p| p.extensions().get::<TraceId>().copied())
            .unwrap_or_else(|| TraceId::generate());
        extensions.insert(trace_id);
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

    fn on_enter(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let span = ctx
            .span(id)
            .expect("span passed to on_enter is open, valid, and stored by subscriber");
        if let Some(timings) = span.extensions_mut().get_mut::<Timings>() {
            if timings.entered_depth == 0 {
                let now = Instant::now();
                timings.idle += now.saturating_duration_since(timings.last).as_nanos() as u64;
                timings.last = now;
            }
            timings.entered_depth = timings.entered_depth.saturating_add(1);
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let span = ctx
            .span(id)
            .expect("span passed to on_exit is open, valid, and stored by subscriber");
        if let Some(timings) = span.extensions_mut().get_mut::<Timings>() {
            if timings.entered_depth == 0 {
                let now = Instant::now();
                timings.busy += now.saturating_duration_since(timings.last).as_nanos() as u64;
                timings.last = now;
            }
            timings.entered_depth = timings.entered_depth.saturating_sub(0);
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let span = event.parent().and_then(|id| ctx.span(id)).or_else(|| {
            event
                .is_contextual()
                .then(|| ctx.lookup_current())
                .flatten()
        });
        let timestamp = Utc::now();
        // removed: tracing-log support by calling .normalized_metadata()
        let meta = event.metadata();
        let mut fields = Fields::new(self.extra_fields.clone());
        if let Some(ref associated_span) = span {
            for span in associated_span.scope().from_root() {
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
        event.record(&mut fields);
        // don't care if channel closed. if capacity is reached, we have larger problems
        let _ = self.sender.try_send(Some(HoneycombEvent {
            time: timestamp,
            data: HoneycombEventInner {
                span_id: span.as_ref().map(|s| s.id().into_u64()),
                trace_id: span.and_then(|s| s.extensions().get::<TraceId>().copied()),
                parent_span_id: None,
                service_name: self.service_name.clone(),
                duration_ms: None,
                idle_ns: None,
                busy_ns: None,
                level: level_as_honeycomb_str(meta.level()),
                name: meta.name().to_owned(),
                target: meta.target().to_owned(),
                fields,
            },
        }));
    }

    fn on_close(&self, id: span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let now = Instant::now();
        let timestamp = Utc::now();
        let span = ctx
            .span(&id)
            .expect("span passed to on_new_span is open, valid, and stored by subscriber");
        let mut fields = Fields::new(self.extra_fields.clone());

        let mut duration_ms = None;
        let mut idle_ns = None;
        let mut busy_ns = None;
        if let Some(timings) = span.extensions().get::<Timings>() {
            duration_ms = Some(now.saturating_duration_since(timings.start).as_millis() as u64);
            idle_ns = Some(timings.idle);
            busy_ns = Some(timings.busy);
        }

        let meta = span.metadata();
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
        span.extensions_mut().remove::<Fields>();
        let trace_id = span.extensions_mut().remove::<TraceId>();

        let _ = self.sender.try_send(Some(HoneycombEvent {
            time: timestamp,
            data: HoneycombEventInner {
                span_id: Some(id.into_u64()),
                trace_id,
                parent_span_id: parent_id.map(|id| id.into_u64()),
                service_name: self.service_name.clone(),
                duration_ms,
                idle_ns,
                busy_ns,
                level: level_as_honeycomb_str(meta.level()),
                name: meta.name().to_owned(),
                target: meta.target().to_owned(),
                fields,
            },
        }));
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::event_channel;
    use serde_json::{Value, json};
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;

    pub(crate) const OTEL_FIELD_SPAN_ID: &'static str = "trace.span_id";
    // const OTEL_FIELD_TRACE_ID: &'static str = "trace.trace_id";
    pub(crate) const OTEL_FIELD_PARENT_ID: &'static str = "trace.parent_id";
    pub(crate) const OTEL_FIELD_SERVICE_NAME: &'static str = "service.name";
    pub(crate) const OTEL_FIELD_LEVEL: &'static str = "level";
    pub(crate) const OTEL_FIELD_TIMESTAMP: &'static str = "timestamp";
    // const OTEL_FIELD_DURATION_MS: &'static str = "duration_ms";

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
        let (sender, mut receiver) = event_channel(16384);
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
                .map(|evt| evt.data.trace_id)
                .collect::<Vec<_>>(),
            std::iter::repeat_n(Some(events[0].data.trace_id.unwrap()), events.len())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            events
                .iter()
                .map(|evt| (evt.data.parent_span_id, evt.data.span_id.unwrap()))
                .collect::<Vec<_>>(),
            vec![
                (None, child_id.into_u64(),),                             // the event
                (Some(parent_id.into_u64()), child_id.into_u64(),),       // child_span closing
                (Some(grandparent_id.into_u64()), parent_id.into_u64(),), // parent_span closing
                (None, grandparent_id.into_u64())                         // grandparent_span closing
            ]
        );
        assert_eq!(
            events
                .iter()
                .map(|evt| (evt.data.fields.fields.get("overridden_field")))
                .collect::<Vec<_>>(),
            vec![
                (Some(&json!(or_val_e))),  // the event
                (Some(&json!(or_val_c))),  // child_span closing
                (Some(&json!(or_val_p))),  // parent_span closing
                (Some(&json!(or_val_gp)))  // grandparent_span closing
            ]
        );

        let log_event = &events[0];
        let ev_map = match serde_json::to_value(&log_event.data).unwrap() {
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
        assert_eq!(ev_map.get(OTEL_FIELD_PARENT_ID), None);
        assert_eq!(
            ev_map.get(OTEL_FIELD_SERVICE_NAME),
            Some(&json!("service_name"))
        );
        assert_eq!(ev_map.get(OTEL_FIELD_LEVEL), Some(&json!("info")));
        assert!(
            before <= log_event.time && log_event.time <= after,
            "invalid timestamp: {:#?}",
            ev_map.get(OTEL_FIELD_TIMESTAMP)
        );
        // "name" field is based on line number so cannot be easily checked
        assert_eq!(
            ev_map.get("target"),
            Some(&json!(format!(
                "{}::layer::tests",
                env!("CARGO_CRATE_NAME")
            )))
        );

        assert_eq!(ev_map.get("event_field"), Some(&json!(e_val)));
        assert_eq!(ev_map.get("child_field"), Some(&json!(c_val)));
        assert_eq!(ev_map.get("parent_field"), Some(&json!(p_val)));
        assert_eq!(ev_map.get("grandparent_field"), Some(&json!(gp_val)));
        assert_eq!(ev_map.get("overridden_field"), Some(&json!(or_val_e)));

        let child_closing_event = events.get(1).unwrap();
        assert_eq!(
            child_closing_event.data.fields.fields.get("child_field"),
            Some(&json!(42))
        );

        let parent_closing_event = &events[2];
        let ev_map = match serde_json::to_value(&parent_closing_event.data).unwrap() {
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

    #[test]
    fn explicit_parent() {
        let (sender, mut receiver) = event_channel(16384);
        let mut layer = Layer {
            extra_fields: Default::default(),
            service_name: Some("service_name".to_owned()),
            sender,
        };
        layer
            .extra_fields
            .insert("my_extra_field".to_owned(), json!("extra_field_val"));
        let subscriber = tracing_subscriber::registry().with(layer);

        let parent_id = tracing::subscriber::with_default(subscriber, || {
            let active_span = span!(Level::INFO, "active_span");
            let _guard = active_span.enter();

            let parent = span!(Level::INFO, "explicit_parent", parent_field = 40);
            let parent_id = parent.id().unwrap();
            tracing::event!(parent: &parent_id, Level::INFO, "message");

            parent_id
        });

        // assert_eq!(receiver.len(), 1);
        let mut events = Vec::with_capacity(1);
        assert_eq!(receiver.blocking_recv_many(&mut events, 128), 3);
        let event = events[0].take().unwrap();
        assert_eq!(
            event.data.fields.fields.get("message"),
            Some(&json!("message"))
        );
        assert_eq!(event.data.parent_span_id, None);
        assert_eq!(event.data.span_id, Some(parent_id.into_u64()));
    }
}
