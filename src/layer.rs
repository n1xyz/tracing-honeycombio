use chrono::Utc;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{Subscriber, span};
use tracing_subscriber::registry::LookupSpan;

use crate::{Fields, HoneycombEvent, HoneycombEventInner};

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
            time: timestamp,
            data: HoneycombEventInner {
                span_id: span_id.map(|id| id.into_u64()),
                parent_id: parent_id.map(|id| id.into_u64()),
                service_name: self.service_name.clone(),
                level: meta.level().as_str(),
                name: meta.name().to_owned(),
                target: meta.target().to_owned(),
                fields,
            },
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
            time: timestamp,
            data: HoneycombEventInner {
                span_id: Some(id.into_u64()),
                parent_id: parent_id.map(|id| id.into_u64()),
                service_name: self.service_name.clone(),
                level: meta.level().as_str(),
                name: meta.name().to_owned(),
                target: meta.target().to_owned(),
                fields,
            },
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_channel;
    use chrono::DateTime;
    use serde_json::{Value, json};
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;

    const OTEL_FIELD_SPAN_ID: &'static str = "trace.span_id";
    // const OTEL_FIELD_TRACE_ID: &'static str = "trace.trace_id";
    const OTEL_FIELD_PARENT_ID: &'static str = "trace.parent_id";
    const OTEL_FIELD_SERVICE_NAME: &'static str = "service.name";
    const OTEL_FIELD_LEVEL: &'static str = "level";
    const OTEL_FIELD_TIMESTAMP: &'static str = "timestamp";
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
                .map(|evt| (evt.data.parent_id, evt.data.span_id.unwrap(),))
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
                .map(|evt| (evt.data.fields.fields.get("overridden_field")))
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
}
