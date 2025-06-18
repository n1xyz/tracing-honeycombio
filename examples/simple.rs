use tracing_subscriber::layer::SubscriberExt;

#[tokio::main]
async fn main() {
    let api_key = std::env::var("HONEYCOMB_API_KEY")
        .expect("HONEYCOMB_API_KEY environment variable to be valid");
    println!("{:#?}", api_key);
    let (layer, task, controller) = tracing_honeycombio::builder(&api_key)
        .build(tracing_honeycombio::HONEYCOMB_SERVER_US, "test")
        .unwrap();
    let handle = tokio::spawn(task);
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();

    tracing::event!(tracing::Level::INFO, value = 42, "hello world!");

    controller.shutdown().await;
    let _ = handle.await;
}
