
use akd_mysql::mysql::*;
use akd::ecvrf::HardCodedAkdVRF;
use std::time::Instant;
use akd::storage::Storage;
use winter_math::fields::f128::BaseElement;
use winter_crypto::hashers::Blake3_256;
use akd::Directory;
use akd::AkdLabel;
use bytes::{BufMut, BytesMut};
use akd::AkdValue;

type Blake3 = Blake3_256<BaseElement>;

/// Size of [AkdLabel]s and [AkdValue]s.
const LABEL_VALUE_SIZE_BYTES: usize = 32;

/// Number of key entries in a large batch.
const LARGE_BATCH_SIZE: u64 = 100;

/// Number of epochs equal to numebr of publish operations.
const NUM_EPOCHS: u64 = 10;

#[tokio::main]

async fn main() {
    maybe_publish_multi_epoch(LARGE_BATCH_SIZE, NUM_EPOCHS).await;
}

pub async fn maybe_publish_multi_epoch(batch_size: u64, num_epoch: u64) {
    if !AsyncMySqlDatabase::test_guard() {
        panic!("Docker container not running?");
    }
    let mysql_db = AsyncMySqlDatabase::new(
        "localhost",
        "test_db",
        Option::from("root"),
        Option::from("example"),
        Option::from(8001),
        MySqlCacheOptions::None,
        200,
    )
    .await;

    // Pre clean-up.
    if let Err(error) = mysql_db.delete_data().await {
        println!("Error cleaning mysql prior to test suite: {}", error);
    }

    // Main call.
    publish_multi_epoch(&mysql_db, batch_size, num_epoch).await;

    // Post clean-up.
    if let Err(mysql_async::Error::Server(error)) = mysql_db.drop_tables().await {
        println!(
            "ERROR: Failed to clean MySQL test database with error {}",
            error
        );
    }
}


pub async fn publish_multi_epoch<S: Storage + Sync + Send>(db: &S, batch_size: u64, num_epoch: u64) {
    // AKD Setup
    let vrf = HardCodedAkdVRF {};
    let akd = Directory::new::<Blake3>(db, &vrf, false).await.unwrap();

    // Generate necessary keys
    let key_entries = generate_key_entries(batch_size * num_epoch);

    for epoch in 0..num_epoch {
        // Determine which subset of keys to publish based on current epoch.
        let publish_index_start: usize = (epoch * batch_size) as usize;
        let publish_index_end: usize = (publish_index_start + (batch_size as usize)) as usize;
        let key_entries_to_publish = &key_entries[publish_index_start..publish_index_end];

        println!("***********************************************************");
        // TODO(eoz): Remove for large batch sizes.
        // println!(
        //     "Key entries to publish in range [{}, {}]: {:?}",
        //     publish_index_start, publish_index_end, key_entries_to_publish
        // );

        let now = Instant::now();
        // Publish
        akd.publish::<Blake3>(key_entries_to_publish.to_vec())
            .await
            .unwrap();

        // Measure elapsed time for publish operation.
        let elapsed = now.elapsed().as_millis() as f64;
        println!(
            "Elapsed time for publishing keys in range [{}, {}]: {} ms.",
            publish_index_start, publish_index_end, elapsed
        );

        // Log database metrics.
        db.log_metrics(log::Level::Error).await;

        // TODO(eoz): Get storage usage
    }
}

pub fn generate_key_entries(num_entries: u64) -> Vec<(AkdLabel, AkdValue)> {
    let mut label = BytesMut::with_capacity(LABEL_VALUE_SIZE_BYTES);
    let mut value = BytesMut::with_capacity(LABEL_VALUE_SIZE_BYTES);

    (0..num_entries)
        .map(|i| {
            label.put_u64(i);
            label.resize(LABEL_VALUE_SIZE_BYTES, 0u8);
            let l = label.split().freeze();

            value.put_u64(i);
            value.resize(LABEL_VALUE_SIZE_BYTES, 0u8);
            let v = value.split().freeze();

            (AkdLabel(l.to_vec()), AkdValue(v.to_vec()))
        })
        .collect()
}
