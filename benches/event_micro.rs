use criterion::{Criterion, criterion_group, criterion_main};
use quanta::Instant;
use std::{
    borrow::Cow,
    hint::black_box,
    pin::Pin,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::Duration,
};
use tokio::sync::mpsc;
use tracing_honeycombio::{
    CreateEventsPayload, Value,
    background::{Backend, BackgroundTaskFut},
    builder::DEFAULT_CHANNEL_SIZE,
};
use tracing_subscriber::layer::SubscriberExt;

struct NoopBackend;

impl Backend for NoopBackend {
    type Err = Box<dyn std::error::Error>;
    type Fut = Pin<Box<dyn Future<Output = Result<(), Self::Err>> + Send + 'static>>;

    fn submit_events<'a>(&mut self, payload: CreateEventsPayload<'a>) -> Self::Fut {
        black_box(payload);
        Box::pin(async { Ok(()) })
    }
}

fn dummy_raw_waker() -> RawWaker {
    fn clone(_: *const ()) -> RawWaker {
        dummy_raw_waker()
    }
    fn wake(_: *const ()) {}
    fn wake_by_ref(_: *const ()) {}
    fn drop(_: *const ()) {}

    let vtable = &RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    RawWaker::new(std::ptr::null(), vtable)
}

fn dummy_waker() -> Waker {
    unsafe { Waker::from_raw(dummy_raw_waker()) }
}

#[inline]
fn single_event() {
    tracing::info!(my_value = "some string", "hello world");
}

pub fn criterion_benchmark(c: &mut Criterion) {
    // note: work neeeded if criterion.rs ever starts using tracing internally
    c.bench_function("single_event", |b| {
        b.iter_custom(|iters| {
            let extra_fields: Vec<(Cow<'static, str>, Value)> = vec![
                ("field1".into(), Cow::Borrowed("value1").into()),
                ("field2".into(), "longer val".repeat(4).into()),
            ];

            let (sender, receiver) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let layer = tracing_honeycombio::layer::Layer::new(
                black_box(Some("my-cool-service-name".into())),
                black_box(sender.clone()),
            );
            let subscriber = tracing_subscriber::registry().with(layer);
            let mut background_task = BackgroundTaskFut::new_with_backend(
                black_box(extra_fields),
                black_box(NoopBackend),
                black_box(receiver),
            );
            // no need to make backgroundtask controller since we will manually use blocking_send
            let waker = dummy_waker();
            let mut cx = Context::from_waker(&waker);

            tracing::subscriber::with_default(subscriber, || {
                let mut elapsed: Duration = Duration::new(0, 0);
                let mut last = Instant::now();
                let its_per_poll: u64 = 100;
                for _ in 0..(iters / its_per_poll) {
                    // timed section
                    for _ in 0..its_per_poll {
                        single_event();
                    }
                    // end timed section; pause clock
                    let now = Instant::now();
                    elapsed += now.duration_since(last);

                    // keep the receiver from falling behind so that the channel doesn't fill up
                    let poll = Pin::new(&mut background_task).poll(&mut cx);
                    debug_assert!(matches!(poll, Poll::Pending));

                    // resume clock
                    last = Instant::now();
                }
                sender.blocking_send(None).unwrap();
                #[allow(clippy::while_let_loop)]
                loop {
                    // we won't panic on tokio sleep() creation without active runtime
                    // because backend always returns Ok so no backoff needed
                    match Pin::new(&mut background_task).poll(&mut cx) {
                        Poll::Pending => {}
                        Poll::Ready(()) => break,
                    }
                }

                elapsed
            })
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
