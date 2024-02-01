use std::env;
use tokio;

use ew::core::tracking::Tracker;
use ew::core::Node;
use ew::monitor::Monitor;
use ew::workers;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Gives some time to tracing subscriber
async fn sleep_some() {
    let ms = 100;
    tracing::debug!("sleeping {ms}ms");
    tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
}

#[tokio::main]
async fn main() -> Result<(), &'static str> {
    // Configure tracing subscriber
    let filter = env::var("EW_LOG").unwrap_or(String::from("ew=info"));
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(filter)
        .finish();

    let _guard = tracing::subscriber::set_global_default(subscriber);

    tracing::info!("starting ew v{VERSION}");
    if cfg!(feature = "test-utilities") {
        tracing::warn!("build includes test-utilities, use cargo's `--no-default-features` flag");
    } else {
        tracing::debug!("compiled without test-utilities");
    }

    // Env variables
    let pg_uri = env::var("EW_POSTGRES_URI").unwrap();
    tracing::debug!("found EW_POSTGRES_URI environment variable");

    let node_url = env::var("EW_NODE_URL").unwrap();
    tracing::debug!("found EW_NODE_URL environment variable");

    let mut monitor = Monitor::new();

    tracing::info!("configuring tracker");
    let node = Node::new("local-node", &node_url);
    let pgconf = ew::config::PostgresConfig::new(&pg_uri);
    let mut tracker = Tracker::new(node, pgconf.clone(), monitor.sender()).await;

    // Workers
    let mut timestamps =
        workers::timestamps::Worker::new("timestamps", &pgconf, &mut tracker, monitor.sender())
            .await;

    let mut network =
        workers::network::Worker::new("network", &pgconf, &mut tracker, monitor.sender()).await;

    let mut erg_diffs =
        workers::erg_diffs::Worker::new("erg_diffs", &pgconf, &mut tracker, monitor.sender()).await;
    let mut erg_diffs_query_handler = workers::erg_diffs::QueryWorker::new(&pgconf).await;

    let mut erg = workers::erg::Worker::new("erg", &pgconf, &mut erg_diffs, monitor.sender()).await;

    let mut cex =
        workers::exchanges::Worker::new("cex", &pgconf, &mut erg_diffs, monitor.sender()).await;
    cex.connect_query_sender(&erg_diffs_query_handler);

    let mut tokens =
        workers::tokens::Worker::new("tokens", &pgconf, &mut tracker, monitor.sender()).await;

    let mut sigmausd =
        workers::sigmausd::Worker::new("sigmausd", &pgconf, &mut tracker, monitor.sender()).await;

    let mut coingecko =
        workers::coingecko::Worker::new(&pgconf, &mut tracker, monitor.sender(), None).await;

    // Start monitor
    tokio::spawn(async move {
        monitor.start().await;
        sleep_some().await;
    });

    // Start tracker
    tokio::spawn(async move {
        sleep_some().await;
        tracker.start().await;
    });

    // Start workers
    tokio::spawn(async move {
        timestamps.start().await;
    });
    tokio::spawn(async move {
        network.start().await;
    });
    tokio::spawn(async move {
        erg_diffs_query_handler.start().await;
    });
    tokio::spawn(async move {
        erg_diffs.start().await;
    });
    tokio::spawn(async move {
        erg.start().await;
    });
    tokio::spawn(async move {
        cex.start().await;
    });
    tokio::spawn(async move {
        tokens.start().await;
    });
    tokio::spawn(async move {
        sigmausd.start().await;
    });
    tokio::spawn(async move {
        coingecko.start().await;
    });

    // Wait for ctrl-c
    _ = tokio::signal::ctrl_c().await;
    for i in [3, 2, 1] {
        tracing::info!("stopping in {i}");
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }
    tracing::info!("exiting");
    Ok(())
}
