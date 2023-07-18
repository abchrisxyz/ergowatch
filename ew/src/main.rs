use std::env;
use tokio;

use ew::core::tracking::Tracker;
use ew::core::types::Head;
use ew::core::Node;
use ew::units;

/// Gives some time to tracing subscriber
async fn sleep_some() {
    let ms = 100;
    tracing::info!("sleeping {ms}ms");
    tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
}

#[tokio::main]
async fn main() -> Result<(), &'static str> {
    // Configure tracing subscriber
    let level = match env::var("EW_LOG_DEBUG") {
        Ok(_) => tracing::Level::DEBUG,
        _ => tracing::Level::INFO,
    };
    tracing::info!("found EW_POSTGRES_URI environment variable");
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_max_level(level)
        .finish();
    let _guard = tracing::subscriber::set_global_default(subscriber);

    tracing::info!("starting EW");

    // Env variables
    let pg_uri = env::var("EW_POSTGRES_URI").unwrap();
    tracing::info!("found EW_POSTGRES_URI environment variable");

    let node_url = env::var("EW_NODE_URL").unwrap();
    tracing::info!("found EW_NODE_URL environment variable");

    tracing::info!("configuring tracker");
    let node = Node::new("local-node", &node_url);
    let pgconf = ew::config::PostgresConfig::new(&pg_uri);
    let mut tracker = Tracker::new(node, pgconf.clone()).await;

    // Dummy sink for now
    // let mut rx = tracker.add_cursor("main".to_string(), Head::initial());

    let mut sigmausd = units::sigmausd::SigWorker::new("sigmausd", &pgconf, &mut tracker).await;
    // let mut sigmausd = units::sigmausd::SigWorker::new(rx);

    // Start tracker
    tokio::spawn(async move {
        sleep_some().await;
        tracker
            .start()
            // .instrument(tracing::info_span!("tracker"))
            .await;
    });

    // Start units
    tokio::spawn(async move {
        sigmausd.start().await;
    });

    // // Dummy sink implementation
    // tracing::info!("starting dummy sink");
    // loop {
    //     tokio::select! {
    //         _ = tokio::signal::ctrl_c() => {break;},
    //         msg = rx.recv() => {
    //             let s = match msg.expect("message is some") {
    //                 ew::core::tracking::TrackingMessage::Genesis(_) => String::from("genesis"),
    //                 ew::core::tracking::TrackingMessage::Include(d) => format!("include {}", d.block.header.height),
    //                 ew::core::tracking::TrackingMessage::Rollback(h) => format!("rollback {}", h),
    //             };
    //             tracing::debug!("dummy sink got message: {}", s);
    //         },
    //     }
    // }

    // Wait for ctrl-c
    _ = tokio::signal::ctrl_c().await;
    tracing::info!("exiting");
    todo!("clean shutdown");
    Ok(())
}
