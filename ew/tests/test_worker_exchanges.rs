mod db_utils;

use db_utils::TestDB;

use ew::framework::store::PgMigrator;
use ew::framework::store::Revision;
use ew::framework::QueryHandler;
use tokio::sync::mpsc;
use tokio_postgres::Client;

use ew::constants::GENESIS_TIMESTAMP;
use ew::constants::ZERO_HEADER;
use ew::core::types::AddressID;
use ew::core::types::Timestamp;
use ew::framework::EventHandling;
use ew::framework::QuerySender;
use ew::framework::QueryWrapper;
use ew::framework::Querying;
use ew::framework::StampedData;
use ew::workers::erg_diffs::queries::DiffsQuery;
use ew::workers::erg_diffs::queries::DiffsQueryResponse;
use ew::workers::erg_diffs::types::DiffData;
use ew::workers::erg_diffs::types::DiffRecord;
use ew::workers::erg_diffs::types::SupplyDiff;
use ew::workers::exchanges::CexWorkFlow;
use ew::workers::exchanges::SupplyRecord;

const TS_10K: Timestamp = 1563159993440; // timestamp of block 10000

pub fn set_tracing_subscriber(set: bool) -> Option<tracing::dispatcher::DefaultGuard> {
    if !set {
        return None;
    }
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter("ew=trace")
        .finish();
    Some(tracing::subscriber::set_default(subscriber))
}

struct MockQueryHandler {
    query_tx: mpsc::Sender<QueryWrapper<DiffsQuery, DiffsQueryResponse>>,
    query_rx: mpsc::Receiver<QueryWrapper<DiffsQuery, DiffsQueryResponse>>,
}

impl MockQueryHandler {
    pub fn new() -> Self {
        let (query_tx, query_rx) = mpsc::channel(8);
        Self { query_tx, query_rx }
    }

    pub fn connect(&self) -> mpsc::Sender<QueryWrapper<DiffsQuery, DiffsQueryResponse>> {
        tracing::debug!("providing a connection to mock query handler");
        self.query_tx.clone()
    }

    /// Waits for next query and responds with given `response`.
    pub async fn handle_next(&mut self, response: DiffsQueryResponse) {
        println!("waiting for next query");
        let qw = self.query_rx.recv().await.unwrap();
        println!("handling query for {:?}", qw.query);
        qw.response_tx.send(response).unwrap();
    }
}

#[tokio::test]
async fn test_empty_blocks() {
    let _guard = set_tracing_subscriber(false);
    let test_db = TestDB::new("exchanges_empty_blocks").await;

    // Genesis
    let genesis_data = StampedData {
        height: 0,
        timestamp: GENESIS_TIMESTAMP,
        header_id: ZERO_HEADER.to_owned(),
        parent_id: "".to_owned(),
        data: DiffData {
            diff_records: vec![],
        },
    };

    // Next block
    let data_1 = genesis_data.wrap_as_child(DiffData {
        diff_records: vec![],
    });

    // Configure worker
    let mut workflow = CexWorkFlow::new(&test_db.pgconf).await;
    let mock_query_handler = MockQueryHandler::new();
    workflow.set_query_sender(QuerySender::new(mock_query_handler.connect()));

    // Process blocks
    workflow.include_block(&genesis_data).await;
    workflow.include_block(&data_1).await;
}

#[tokio::test]
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);
    let addr_a = AddressID(1001);
    let addr_b = AddressID(2001);
    let test_db = TestDB::new("exchanges_rollback").await;
    test_db.init_core().await;

    // Init dummy workflow to initialize test db so we can fill in mock data
    let _ = CexWorkFlow::new(&test_db.pgconf).await;

    // Define some fake CEX's in the test db
    let cex1_address = AddressID(9101);
    let cex1_id: i32 = 10000;
    insert_exchange(&test_db.client, cex1_id, "Exchange 1", "cex_1").await;
    insert_main_address(&test_db.client, cex1_id, &cex1_address).await;
    let cex2_address = AddressID(9201);
    let cex2_id: i32 = 20000;
    insert_exchange(&test_db.client, cex2_id, "Exchange 2", "cex_2").await;
    insert_main_address(&test_db.client, cex2_id, &cex2_address).await;

    // Genesis
    let genesis_data = StampedData {
        height: 0,
        timestamp: GENESIS_TIMESTAMP,
        header_id: ZERO_HEADER.to_owned(),
        parent_id: "".to_owned(),
        data: DiffData {
            diff_records: vec![],
        },
    };

    // Block 1
    let data_1 = genesis_data
        .wrap_as_child(DiffData {
            diff_records: vec![
                // Creating A and B out of thin air
                DiffRecord::new(addr_a, 1, 0, 20_000_000_000),
                DiffRecord::new(addr_b, 1, 0, 30_000_000_000),
                // A sends 5 to cex1, so A is a deposit address
                DiffRecord::new(addr_a, 1, 1, -5_000_000_000),
                DiffRecord::new(cex1_address, 1, 1, 5_000_000_000),
            ],
        })
        .timestamp(TS_10K);

    // Block 2
    let data_2 = data_1
        .wrap_as_child(DiffData {
            diff_records: vec![
                // A sends 5 to cex2 --> conflict
                DiffRecord::new(addr_a, 2, 0, -5_000_000_000),
                DiffRecord::new(cex2_address, 2, 0, 5_000_000_000),
                // B sends 1 to cex1 --> deposit
                DiffRecord::new(addr_b, 2, 1, -1_000_000_000),
                DiffRecord::new(cex1_address, 2, 1, 1_000_000_000),
            ],
        })
        .timestamp(data_1.timestamp + 120_000);

    // Register core header for parent of rolled back blocks
    test_db.insert_core_header(&data_1.get_header()).await;

    // Configure workflow
    let mut workflow = CexWorkFlow::new(&test_db.pgconf).await;
    let mut mock_query_handler = MockQueryHandler::new();
    workflow.set_query_sender(QuerySender::new(mock_query_handler.connect()));

    // Spawn mock query handler.
    tokio::spawn(async move {
        // Patches apply to current block too.
        // Block 1 - querying for addr_a spotted as deposit
        mock_query_handler
            .handle_next(vec![SupplyDiff::new(1, 15_000_000_000)]) // A (+ 20 - 5)
            .await;
        // Block 2 - querying for addr_b spotted as deposit and addr_a as conflict
        mock_query_handler
            .handle_next(vec![
                SupplyDiff::new(1, 30_000_000_000), // B +30
                SupplyDiff::new(2, -1_000_000_000), // B -1
            ])
            .await;
        mock_query_handler
            .handle_next(vec![
                SupplyDiff::new(1, 15_000_000_000), // A +15
                SupplyDiff::new(2, -5_000_000_000), // A -5
            ])
            .await;
        // Rollback 2
        mock_query_handler
            .handle_next(vec![
                SupplyDiff::new(1, 15_000_000_000), // A +15
                SupplyDiff::new(2, -5_000_000_000), // A -5
            ])
            .await;
        mock_query_handler
            .handle_next(vec![
                SupplyDiff::new(1, 30_000_000_000), // B +30
                SupplyDiff::new(2, -1_000_000_000), // B -1
            ])
            .await;
    });

    // Process blocks
    workflow.include_block(&genesis_data).await;
    workflow.include_block(&data_1).await;
    workflow.include_block(&data_2).await;

    // Check db state before rollback
    let deposit_addresses = get_deposit_addresses(&test_db.client).await;
    assert_eq!(deposit_addresses, vec![addr_b]);
    let deposit_conflicts = get_deposit_conflicts(&test_db.client).await;
    assert_eq!(deposit_conflicts, vec![addr_a]);
    let supply_records = get_supply_records(&test_db.client).await;
    assert_eq!(
        supply_records,
        vec![
            SupplyRecord {
                height: 0,
                main: 0,
                deposits: 0
            },
            SupplyRecord {
                height: 1,
                main: 5_000_000_000,
                deposits: 30_000_000_000, // B only as A is a conflict
            },
            SupplyRecord {
                height: 2,
                main: 11_000_000_000,
                deposits: 29_000_000_000
            }
        ]
    );

    // Do the rollback
    workflow.roll_back(data_2.height).await;

    // Check db state after rollback
    let deposit_addresses = get_deposit_addresses(&test_db.client).await;
    assert_eq!(deposit_addresses, vec![addr_a]);
    let deposit_conflicts = get_deposit_conflicts(&test_db.client).await;
    assert_eq!(deposit_conflicts, vec![]);
}

#[tokio::test]
async fn test_reproduce_negative_deposits_bug() {
    let _guard = set_tracing_subscriber(false);
    let addr_a = AddressID(8581);
    let addr_b = AddressID(8711);
    let addr_c = AddressID(8861);
    let addr_d = AddressID(9051);
    let test_db = TestDB::new("exchanges_neg_deps_bug").await;
    test_db.init_core().await;

    // Init dummy workflow to initialize test db so we can fill in mock data
    let _ = CexWorkFlow::new(&test_db.pgconf).await;

    // Define a fake CEX in the test db
    let cex1_address = AddressID(9101);
    let cex1_id: i32 = 10000;
    insert_exchange(&test_db.client, cex1_id, "Exchange 1", "cex_1").await;
    insert_main_address(&test_db.client, cex1_id, &cex1_address).await;

    // Genesis
    let genesis_data = StampedData {
        height: 0,
        timestamp: GENESIS_TIMESTAMP,
        header_id: ZERO_HEADER.to_owned(),
        parent_id: "".to_owned(),
        data: DiffData {
            diff_records: vec![],
        },
    };

    // Block 1
    let data_1 = genesis_data.wrap_as_child(DiffData {
        diff_records: vec![],
    });

    // Block 2
    let data_2 = data_1.wrap_as_child(DiffData {
        diff_records: vec![DiffRecord::new(addr_a, 2, 0, 2487000000)],
    });

    let data_3 = data_2.wrap_as_child(DiffData {
        diff_records: vec![DiffRecord::new(addr_b, 3, 0, 10000000000)],
    });
    let data_4 = data_3.wrap_as_child(DiffData {
        diff_records: vec![DiffRecord::new(addr_b, 4, 0, 18590000000000)],
    });
    let data_5 = data_4.wrap_as_child(DiffData {
        diff_records: vec![
            DiffRecord::new(addr_a, 5, 0, -2487000000),
            DiffRecord::new(addr_b, 5, 0, -18600000000000),
            DiffRecord::new(cex1_address, 5, 0, 18602487000000),
        ],
    });
    let data_6 = data_5.wrap_as_child(DiffData {
        diff_records: vec![DiffRecord::new(addr_c, 6, 0, 100000000000)],
    });
    let data_7 = data_6.wrap_as_child(DiffData {
        diff_records: vec![DiffRecord::new(addr_c, 7, 0, 3882005962830)],
    });
    let data_8 = data_7.wrap_as_child(DiffData {
        diff_records: vec![
            DiffRecord::new(addr_c, 8, 0, -3982005962830),
            DiffRecord::new(cex1_address, 8, 0, 3982005962830),
        ],
    });
    let data_9 = data_8.wrap_as_child(DiffData {
        diff_records: vec![DiffRecord::new(addr_d, 9, 0, 1000000000000)],
    });
    let data_10 = data_9.wrap_as_child(DiffData {
        diff_records: vec![
            DiffRecord::new(addr_d, 10, 0, -1000000000000),
            DiffRecord::new(cex1_address, 10, 0, 1000000000000),
        ],
    });

    // Prepare erg.balance_diffs table
    test_db
        .init_schema(include_str!("../src/workers/erg_diffs/store/schema.sql"))
        .await;
    insert_balance_diffs(&test_db.client, &data_1.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_2.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_3.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_4.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_5.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_6.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_7.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_8.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_9.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_10.data.diff_records).await;

    // Configure workflow
    let mut workflow = CexWorkFlow::new(&test_db.pgconf).await;
    let mut query_handler = ew::workers::erg_diffs::QueryWorker::new(&test_db.pgconf).await;
    workflow.set_query_sender(query_handler.connect());

    // Spawn query handler.

    tokio::spawn(async move {
        query_handler.start().await;
    });

    // Process blocks
    workflow.include_block(&genesis_data).await;
    workflow.include_block(&data_1).await;
    workflow.include_block(&data_2).await;
    workflow.include_block(&data_3).await;
    workflow.include_block(&data_4).await;
    workflow.include_block(&data_5).await;
    workflow.include_block(&data_6).await;
    workflow.include_block(&data_7).await;
    workflow.include_block(&data_8).await;
    workflow.include_block(&data_9).await;
    workflow.include_block(&data_10).await;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Check db state
    let supply_records = get_supply_records(&test_db.client).await;
    assert_eq!(supply_records.len(), 11);
    assert_eq!(
        supply_records[0],
        SupplyRecord {
            height: 0,
            main: 0,
            deposits: 0
        }
    );
    assert_eq!(
        supply_records[1],
        SupplyRecord {
            height: 1,
            main: 0,
            deposits: 0,
        }
    );
    assert_eq!(
        supply_records[2],
        SupplyRecord {
            height: 2,
            main: 0,
            deposits: 2487000000,
        }
    );
    assert_eq!(
        supply_records[3],
        SupplyRecord {
            height: 3,
            main: 0,
            deposits: 12487000000,
        }
    );
    assert_eq!(
        supply_records[4],
        SupplyRecord {
            height: 4,
            main: 0,
            deposits: 18602487000000,
        }
    );
    assert_eq!(
        supply_records[5],
        SupplyRecord {
            height: 5,
            main: 18602487000000,
            deposits: 0,
        }
    );
    assert_eq!(
        supply_records[6],
        SupplyRecord {
            height: 6,
            main: 18602487000000,
            deposits: 100000000000,
        }
    );
    assert_eq!(
        supply_records[7],
        SupplyRecord {
            height: 7,
            main: 18602487000000,
            deposits: 3982005962830
        }
    );
    assert_eq!(
        supply_records[8],
        SupplyRecord {
            height: 8,
            main: 22584492962830,
            deposits: 0,
        }
    );
    assert_eq!(
        supply_records[9],
        SupplyRecord {
            height: 9,
            main: 22584492962830,
            deposits: 1000000000000,
        }
    );
    assert_eq!(
        supply_records[10],
        SupplyRecord {
            height: 10,
            main: 23584492962830,
            deposits: 0
        }
    );
}

#[tokio::test]
async fn test_ignored_deposit_addresses() {
    let _guard = set_tracing_subscriber(false);
    let addr_a = AddressID(8581);
    let addr_b = AddressID(8711);
    let addr_i = AddressID(9051);
    let test_db = TestDB::new("exchanges_ignored_deps").await;
    test_db.init_core().await;

    // Init dummy workflow to initialize test db so we can fill in mock data
    let _ = CexWorkFlow::new(&test_db.pgconf).await;

    // Define a fake CEX in the test db
    let cex1_address = AddressID(9101);
    let cex1_id: i32 = 10000;
    insert_exchange(&test_db.client, cex1_id, "Exchange 1", "cex_1").await;
    insert_main_address(&test_db.client, cex1_id, &cex1_address).await;
    insert_ignored_address(&test_db.client, &addr_i).await;

    // Genesis
    let genesis_data = StampedData {
        height: 0,
        timestamp: GENESIS_TIMESTAMP,
        header_id: ZERO_HEADER.to_owned(),
        parent_id: "".to_owned(),
        data: DiffData {
            diff_records: vec![],
        },
    };

    // Block 1
    let data_1 = genesis_data.wrap_as_child(DiffData {
        diff_records: vec![],
    });

    // Block 2
    let data_2 = data_1.wrap_as_child(DiffData {
        diff_records: vec![DiffRecord::new(addr_a, 2, 0, 2487000000)],
    });

    let data_3 = data_2.wrap_as_child(DiffData {
        diff_records: vec![DiffRecord::new(addr_b, 3, 0, 10000000000)],
    });

    // Block 4 - a sends to i
    let data_4 = data_3.wrap_as_child(DiffData {
        diff_records: vec![
            DiffRecord::new(addr_a, 4, 0, -2487000000),
            DiffRecord::new(addr_i, 4, 0, 2487000000),
        ],
    });

    // Block 5 - i sends to cex but should not be flagged as deposit because on ignore list
    let data_5 = data_4.wrap_as_child(DiffData {
        diff_records: vec![
            DiffRecord::new(addr_i, 5, 0, -2487000000),
            DiffRecord::new(cex1_address, 5, 0, 2487000000),
        ],
    });

    // Prepare erg.balance_diffs table
    test_db
        .init_schema(include_str!("../src/workers/erg_diffs/store/schema.sql"))
        .await;
    insert_balance_diffs(&test_db.client, &data_1.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_2.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_3.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_4.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_5.data.diff_records).await;

    // Configure workflow
    let mut workflow = CexWorkFlow::new(&test_db.pgconf).await;
    let mut query_handler = ew::workers::erg_diffs::QueryWorker::new(&test_db.pgconf).await;
    workflow.set_query_sender(query_handler.connect());

    // Spawn query handler.
    tokio::spawn(async move {
        query_handler.start().await;
    });

    // Process blocks
    workflow.include_block(&genesis_data).await;
    workflow.include_block(&data_1).await;
    workflow.include_block(&data_2).await;
    workflow.include_block(&data_3).await;
    workflow.include_block(&data_4).await;
    workflow.include_block(&data_5).await;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Check db state
    let supply_records = get_supply_records(&test_db.client).await;
    assert_eq!(supply_records.len(), 6);
    assert_eq!(
        supply_records[0],
        SupplyRecord {
            height: 0,
            main: 0,
            deposits: 0
        }
    );
    assert_eq!(
        supply_records[1],
        SupplyRecord {
            height: 1,
            main: 0,
            deposits: 0,
        }
    );
    assert_eq!(
        supply_records[2],
        SupplyRecord {
            height: 2,
            main: 0,
            deposits: 0,
        }
    );
    assert_eq!(
        supply_records[3],
        SupplyRecord {
            height: 3,
            main: 0,
            deposits: 0,
        }
    );
    assert_eq!(
        supply_records[4],
        SupplyRecord {
            height: 4,
            main: 0,
            deposits: 0, // ignored deposit
        }
    );
    assert_eq!(
        supply_records[5],
        SupplyRecord {
            height: 5,
            main: 2487000000,
            deposits: 0,
        }
    );
}

#[tokio::test]
async fn test_intra_block_conflict() {
    let _guard = set_tracing_subscriber(false);
    let addr_a = AddressID(8581);
    let test_db = TestDB::new("exchanges_intra_block_conflict").await;
    test_db.init_core().await;

    // Init dummy workflow to initialize test db so we can fill in mock data
    let _ = CexWorkFlow::new(&test_db.pgconf).await;

    // Define 2 fake CEX's in the test db
    let cex1_address = AddressID(9101);
    let cex1_id: i32 = 10000;
    insert_exchange(&test_db.client, cex1_id, "Exchange 1", "cex_1").await;
    insert_main_address(&test_db.client, cex1_id, &cex1_address).await;
    let cex2_address = AddressID(9102);
    let cex2_id: i32 = 20000;
    insert_exchange(&test_db.client, cex1_id, "Exchange 2", "cex_2").await;
    insert_main_address(&test_db.client, cex2_id, &cex2_address).await;

    // Genesis
    let genesis_data = StampedData {
        height: 0,
        timestamp: GENESIS_TIMESTAMP,
        header_id: ZERO_HEADER.to_owned(),
        parent_id: "".to_owned(),
        data: DiffData {
            diff_records: vec![],
        },
    };

    // Block 1 - create some out of thin air
    let data_1 = genesis_data.wrap_as_child(DiffData {
        diff_records: vec![DiffRecord::new(addr_a, 1, 0, 7_000_000_000)],
    });

    // Block 2 - a sends to cex1 and cex2 --> intra-block conflict
    let data_2 = data_1.wrap_as_child(DiffData {
        diff_records: vec![
            DiffRecord::new(addr_a, 2, 0, -3_000_000_000),
            DiffRecord::new(cex1_address, 2, 0, 2_000_000_000),
            DiffRecord::new(cex2_address, 2, 0, 1_000_000_000),
        ],
    });

    // Block 3 - a sends to cex1 again, should be ignored
    let data_3 = data_2.wrap_as_child(DiffData {
        diff_records: vec![
            DiffRecord::new(addr_a, 3, 0, -2_000_000_000),
            DiffRecord::new(cex1_address, 3, 0, 2_000_000_000),
        ],
    });

    // Prepare erg.balance_diffs table
    test_db
        .init_schema(include_str!("../src/workers/erg_diffs/store/schema.sql"))
        .await;
    insert_balance_diffs(&test_db.client, &data_1.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_2.data.diff_records).await;
    insert_balance_diffs(&test_db.client, &data_3.data.diff_records).await;

    // Configure workflow
    let mut workflow = CexWorkFlow::new(&test_db.pgconf).await;
    let mut query_handler = ew::workers::erg_diffs::QueryWorker::new(&test_db.pgconf).await;
    workflow.set_query_sender(query_handler.connect());

    // Spawn query handler.
    tokio::spawn(async move {
        query_handler.start().await;
    });

    // Process blocks
    workflow.include_block(&genesis_data).await;
    workflow.include_block(&data_1).await;
    workflow.include_block(&data_2).await;
    workflow.include_block(&data_3).await;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Check db state
    let supply_records = get_supply_records(&test_db.client).await;
    assert_eq!(supply_records.len(), 4);
    assert_eq!(
        supply_records[0],
        SupplyRecord {
            height: 0,
            main: 0,
            deposits: 0
        }
    );
    assert_eq!(
        supply_records[1],
        SupplyRecord {
            height: 1,
            main: 0,
            deposits: 0,
        }
    );
    assert_eq!(
        supply_records[2],
        SupplyRecord {
            height: 2,
            main: 3_000_000_000,
            deposits: 0,
        }
    );
    assert_eq!(
        supply_records[3],
        SupplyRecord {
            height: 3,
            main: 5_000_000_000,
            deposits: 0,
        }
    );

    let deposit_conflicts = get_deposit_conflicts(&test_db.client).await;
    assert_eq!(deposit_conflicts, vec![addr_a]);
}

#[tokio::test]
async fn test_migrations() {
    let _guard = set_tracing_subscriber(false);
    // Actual first xeggex address
    let addr_x = AddressID(9336381);
    let test_db = TestDB::new("exchanges_migrations").await;
    test_db.init_core().await;

    // Load initial schema to trigger migrations when store is initialized
    test_db
        .init_schema(include_str!(
            "../src/workers/exchanges/store/schema.1.0.sql"
        ))
        .await;
    // Register schema revision
    test_db.init_ew().await;
    test_db
        .set_revision("exchanges", "exchanges", &Revision::new(1, 0))
        .await;

    // Prepare erg.balance_diffs table
    test_db
        .init_schema(include_str!("../src/workers/erg_diffs/store/schema.sql"))
        .await;
    // Include a diff for xeggex address. Just so we have an existing address for one of the migrations.
    insert_balance_diffs(
        &test_db.client,
        &vec![DiffRecord::new(addr_x, 3, 0, 10000000000)],
    )
    .await;

    // Run migrations
    let mut migrator =
        PgMigrator::new(&test_db.pgconf, &ew::workers::exchanges::testing::SCHEMA).await;
    migrator
        .apply(&ew::workers::exchanges::testing::Mig1_1 {})
        .await;
    migrator
        .apply(&ew::workers::exchanges::testing::Mig1_2 {})
        .await;
    migrator
        .apply(&ew::workers::exchanges::testing::Mig1_3 {})
        .await;

    // Check revision
    let rev = test_db
        .get_revision("exchanges", "exchanges")
        .await
        .expect("revsion should be set");
    assert_eq!(rev.major, 1);
    assert_eq!(rev.minor, 3);
}

async fn insert_exchange(client: &Client, id: i32, name: &str, text_id: &str) {
    let stmt = "
        insert into exchanges.exchanges (id, name, text_id)
        values ($1, $2, $3);
    ";
    client.execute(stmt, &[&id, &name, &text_id]).await.unwrap();
}

async fn insert_main_address(client: &Client, cex_id: i32, address_id: &AddressID) {
    let stmt = "
        insert into exchanges.main_addresses (cex_id, address_id, address)
        values ($1, $2, 'dymmy-address');
    ";
    client.execute(stmt, &[&cex_id, &address_id]).await.unwrap();
}

async fn insert_ignored_address(client: &Client, address_id: &AddressID) {
    let stmt = "
        insert into exchanges.deposit_addresses_ignored (address_id)
        values ($1);
    ";
    client.execute(stmt, &[&address_id]).await.unwrap();
}

async fn get_deposit_addresses(client: &Client) -> Vec<AddressID> {
    client
        .query(
            "
            select address_id
            from exchanges.deposit_addresses
            order by address_id;",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
}

async fn get_deposit_conflicts(client: &Client) -> Vec<AddressID> {
    client
        .query(
            "
            select address_id
            from exchanges.deposit_addresses_excluded
            order by address_id;",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
}

async fn get_supply_records(client: &Client) -> Vec<SupplyRecord> {
    client
        .query(
            "
            select height
                , main
                , deposits
            from exchanges.supply
            order by 1;",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| SupplyRecord {
            height: r.get(0),
            main: r.get(1),
            deposits: r.get(2),
        })
        .collect()
}

async fn insert_balance_diffs(client: &Client, diff_records: &Vec<DiffRecord>) {
    let sql = "
        insert into erg.balance_diffs (address_id, height, tx_idx, nano)
        values ($1, $2, $3, $4);
    ";
    for r in diff_records {
        client
            .execute(sql, &[&r.address_id, &r.height, &r.tx_idx, &r.nano])
            .await
            .unwrap();
    }
}
