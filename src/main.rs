use akd::ecvrf::HardCodedAkdVRF;
use akd::storage::Storage;
use akd::AkdLabel;
use akd::AkdValue;
use akd::Directory;
use akd_mysql::mysql::*;
use bytes::{BufMut, BytesMut};
use csv::Writer;
use std::process::Command;
// use std::time::Duration;
use std::time::Instant;
use winter_crypto::hashers::Blake3_256;
use winter_math::fields::f128::BaseElement;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

type Blake3 = Blake3_256<BaseElement>;

/// Size of [AkdLabel]s and [AkdValue]s.
const LABEL_VALUE_SIZE_BYTES: usize = 32;

/// Number of key entries in a large batch.
const LARGE_BATCH_SIZE: u64 = 1000;

/// Number of epochs equal to numebr of publish operations.
const NUM_EPOCHS: u64 = 10;

/// csv file name to be prepended for the data
const CSV_PREFIX: &str = "./output_csvs/azks_experiment_";

#[tokio::main]

async fn main() {
    run_table_sizes_command();
    maybe_publish_multi_epoch(LARGE_BATCH_SIZE, NUM_EPOCHS).await;
}

pub async fn maybe_publish_multi_epoch(batch_size: u64, num_epoch: u64) {
    if !AsyncMySqlDatabase::test_guard() {
        panic!("Docker container not running?");
    } else {
        println!("Container running.");
    }
    let mysql_db = AsyncMySqlDatabase::new(
        "localhost",
        "test_db",
        Option::from("root"),
        Option::from("example"),
        Option::from(8001),
        MySqlCacheOptions::None,//Specific(Duration::new(120, 0)),
        2000,
    )
    .await;
    println!("Got database connection!");

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

pub async fn publish_multi_epoch<S: Storage + Sync + Send>(
    db: &S,
    batch_size: u64,
    num_epoch: u64,
) {
    let mut filename = CSV_PREFIX.to_owned();
    let mut batch_size_loc = LARGE_BATCH_SIZE.to_string().to_owned();
    batch_size_loc.push_str("_");
    let total_size = (LARGE_BATCH_SIZE * NUM_EPOCHS).to_string().to_owned();
    filename.push_str(&batch_size_loc);
    filename.push_str(&total_size);
    let mut wtr = Writer::from_path(filename).unwrap();

    println!("Publishing...");
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

        update_table_sizes();
        if let Some(table_sizes_output) = run_table_sizes_command() {
            // Output format
            // Table   Size (MB)
            // history 4.41
            // users   1.52
            // azks    0.02
            println!("Table sizes:\n{}", table_sizes_output);
            let lines = table_sizes_output.trim().split("\n");
            // Split by line (skip the line with column names).
            let mut tables_and_sizes_to_log = Vec::new();
            for line in lines.skip(1) {
                // \t is the separator between the columns.
                let tables_or_sizes = line.split("\t").collect::<Vec<_>>();
                if tables_or_sizes.len() != 2 {
                    panic!("2 columns should be there for a table (history, user, or azks) and its size. Current len: {}", tables_or_sizes.len());
                }
                tables_and_sizes_to_log.push(tables_or_sizes[0]);
                tables_and_sizes_to_log.push(tables_or_sizes[1]);
            }
            if tables_and_sizes_to_log.len() != 8 {
                panic!("8 elements should be there for a table (history, user, or azks) and their sizes. Current len: {}. Extra or missing tbales?", tables_and_sizes_to_log.len());
            }
            // Log the data into the CSV.
            wtr.write_record(&[
                publish_index_start.to_string(),
                publish_index_end.to_string(),
                elapsed.to_string(),
                tables_and_sizes_to_log[0].to_string(),
                tables_and_sizes_to_log[1].to_string(),
                tables_and_sizes_to_log[2].to_string(),
                tables_and_sizes_to_log[3].to_string(),
                tables_and_sizes_to_log[4].to_string(),
                tables_and_sizes_to_log[5].to_string(),
                tables_and_sizes_to_log[6].to_string(),
                tables_and_sizes_to_log[7].to_string(),
            ])
            .unwrap();
            wtr.flush().unwrap();
        } else {
            panic!("Table sizes command failed!");
        }

        // Log database metrics.
        db.log_metrics(log::Level::Trace).await;
    }
}

pub fn run_mysql_command(command: &str) -> Option<String> {
    let output = Command::new("mysql")
        .args([
            "-h",
            "127.0.0.1",
            "-P",
            "8001",
            "-u",
            "root",
            "-e",
            &command,
        ])
        .output();
    match &output {
        Ok(result) => {
            if let (Ok(out), Ok(err)) = (
                std::str::from_utf8(&result.stdout),
                std::str::from_utf8(&result.stderr),
            ) {
                if err != "" {
                    println!("Error output not empty: {}", err);
                }
                Some(out.to_string())
            } else {
                println!(
                    "Parsing command output as STDOUT and STDERR failed. Output: {:?}",
                    result
                );
                None
            }
        }
        Err(err) => {
            println!("Table sizes command failed. Error: {:?}", err);
            None
        }
    }
}

pub fn update_table_sizes() {
    if run_mysql_command("ANALYZE TABLE test_db.users;").is_none() {
        panic!("Analyze table users failed!");
    }
    if run_mysql_command("ANALYZE TABLE test_db.history;").is_none() {
        panic!("Analyze table history failed!");
    }
    if run_mysql_command("ANALYZE TABLE test_db.azks;").is_none() {
        panic!("Analyze table azks failed!");
    }
}
pub fn run_table_sizes_command() -> Option<String> {
    run_mysql_command(
        "SELECT
                TABLE_NAME AS `Table`,
                ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024),2) AS `Size (MB)`
            FROM
                information_schema.TABLES
            WHERE
                TABLE_SCHEMA = 'test_db'
            ORDER BY
                (DATA_LENGTH + INDEX_LENGTH)
            DESC;",
    )
}

pub fn generate_key_entries(num_entries: u64) -> Vec<(AkdLabel, AkdValue)> {
    let mut label = BytesMut::with_capacity(LABEL_VALUE_SIZE_BYTES);
    let mut value = BytesMut::with_capacity(LABEL_VALUE_SIZE_BYTES);

    // generate the test data
    let mut rng = thread_rng();

    let mut users: Vec<String> = vec![];
    for _ in 0..num_entries {
        users.push(
            thread_rng()
                .sample_iter(&Alphanumeric)
                .take(32)
                .map(char::from)
                .collect(),
        );
    }
    
    let mut data = Vec::new();
    let fake_value = 42;
    for value in users.iter() {

        data.push((
            AkdLabel::from_utf8_str(value),
            AkdValue(format!("{:?}", value).as_bytes().to_vec()),
        ));
    }
    data
   
}
