use reqwest::header::{self, HeaderMap, HeaderName};

use crate::{
    Url,
    background::{BackgroundTask, BackgroundTaskController},
};
use std::collections::HashMap;

pub const HONEYCOMB_SERVER_US: &'static str = "https://api.honeycomb.io/";
pub const HONEYCOMB_SERVER_EU: &'static str = "https://api.eu1.honeycomb.io/";

/// Builder for constructing a [`Layer`] and its corresponding
/// [`BackgroundTask`].
pub struct Builder {
    pub service_name: Option<String>,
    // TODO: custom value type
    pub extra_fields: HashMap<String, serde_json::Value>,
    pub http_headers: reqwest::header::HeaderMap,
    pub event_channel_size: usize,
}

#[derive(Debug)]
pub enum AddHeaderError {
    InvalidHttpHeaderName(String),
    InvalidHttpHeaderValue(String),
}

impl std::fmt::Display for AddHeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidHttpHeaderName(name) => write!(f, "invalid HTTP header name {:?}", name),
            Self::InvalidHttpHeaderValue(val) => write!(f, "invalid HTTP header value {:?}", val),
        }
    }
}

impl std::error::Error for AddHeaderError {}

#[derive(Debug)]
pub struct InvalidEndpointConfig {
    api_host: String,
    dataset_slug: String,
}

impl std::fmt::Display for InvalidEndpointConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot build Honeycomb endpoint from API host {:?} and dataset {:?}",
            self.api_host, self.dataset_slug
        )
    }
}

impl std::error::Error for InvalidEndpointConfig {}

impl Builder {
    /// Set the logical name of the service, using the `service.name` field as defined
    /// by OpenTelemetry. Used for distributed tracing.
    pub fn service_name(mut self, service_name: String) -> Self {
        self.service_name = Some(service_name);
        self
    }

    /// Insert an extra field that is sent with every event.
    /// The following fields have special meanings and should not be used:
    ///
    /// - `trace.span_id`
    /// - `trace.trace_id`
    /// - `trace.parent_id``
    /// - `service.name`
    /// - `level`
    /// - `Timestamp`
    /// - `name`
    /// - `target`
    /// - `duration_ms`
    pub fn insert_extra_field(mut self, field_name: String, field_val: serde_json::Value) -> Self {
        self.extra_fields.insert(field_name, field_val);
        self
    }

    /// Set an extra HTTP header to be sent with all requests sent to Honeycomb.
    pub fn http_header<S: AsRef<str>, T: AsRef<str>>(
        mut self,
        name: S,
        val: T,
    ) -> Result<Self, AddHeaderError> {
        self.http_headers.insert(
            HeaderName::from_bytes(name.as_ref().as_bytes())
                .map_err(|_| AddHeaderError::InvalidHttpHeaderName(name.as_ref().to_owned()))?,
            val.as_ref()
                .try_into()
                .map_err(|_| AddHeaderError::InvalidHttpHeaderValue(val.as_ref().to_owned()))?,
        );
        Ok(self)
    }

    /// Size of the [`std::sync::mpsc`] channel used to send events from the layer to
    /// the background task. Events are silently dropped if this limit is reached, so
    /// the default is large (`16384`) such that it will only be reached by a buggy
    /// program.
    pub fn event_channel_size(mut self, size: usize) -> Self {
        self.event_channel_size = size;
        self
    }

    /// Build using the US instance.
    /// `dataset_slug` is the case-insensitive Honeycomb dataset to send events to.
    /// Names may contain URL-encoded spaces or other special characters, but not
    /// URL-encoded slashes. For example, "My%20Dataset" will show up in the UI as "My
    /// Dataset".
    pub fn build(
        self,
        api_host: &str,
        dataset_slug: &str,
    ) -> Result<
        (
            crate::layer::Layer,
            BackgroundTask,
            BackgroundTaskController,
        ),
        InvalidEndpointConfig,
    > {
        // endpoint is {api_host}/1/batch/{datasetSlug}
        // ref: https://api-docs.honeycomb.io/api/events/createevents
        let endpoint = Url::parse(api_host)
            .and_then(|host| host.join("1/batch"))
            .and_then(|endpoint| endpoint.join(dataset_slug))
            .map_err(|_| InvalidEndpointConfig {
                api_host: api_host.to_string(),
                dataset_slug: dataset_slug.to_string(),
            })?;
        Ok(self.build_with_custom_endpoint(endpoint))
    }

    /// Build using a custom "Create Events" endpoint [`Url`].
    pub fn build_with_custom_endpoint(
        self,
        honeycomb_endpoint_url: Url,
    ) -> (
        crate::layer::Layer,
        BackgroundTask,
        BackgroundTaskController,
    ) {
        let (sender, receiver) = crate::event_channel(self.event_channel_size);
        let layer = crate::layer::Layer::new(self.extra_fields, self.service_name, sender.clone());
        let background_task =
            BackgroundTask::new(honeycomb_endpoint_url, self.http_headers, receiver);
        let background_controller = BackgroundTaskController::new(sender);
        (layer, background_task, background_controller)
    }
}

/// Create a [`Builder`] with the given `api_key`. Find your team's API key at
/// https://ui.honeycomb.io/account.
///
/// It is recommended that an Ingest API key is used for sending events.
/// A Configuration API key will work, and must have the Send Events permission.
/// Learn more about API keys:
/// https://docs.honeycomb.io/get-started/configure/environments/manage-api-keys/
///
/// Panics if `api_key` is not a valid HTTP header value.
pub fn builder(api_key: &str) -> Builder {
    let mut builder = Builder {
        service_name: None,
        extra_fields: HashMap::new(),
        http_headers: HeaderMap::new(),
        event_channel_size: 16384,
    };
    let mut auth_value =
        header::HeaderValue::from_str(api_key).expect("api_key to be a valid HTTP header value");
    auth_value.set_sensitive(true);
    builder
        .http_headers
        .insert(header::AUTHORIZATION, auth_value);
    builder
}
