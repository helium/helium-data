use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDateTime, Utc};
use clap::Parser;
use datafusion::arrow::array::StringArray;
use deltalake::{
    action::{self, Action, CommitInfo, SaveMode},
    crate_version,
    datafusion::prelude::SessionContext,
    operations::transaction::TransactionError,
    table_state::DeltaTableState,
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps, DeltaTable, DeltaTableBuilder, DeltaTableError, DeltaTableMetaData, ObjectStore,
    Path, SchemaDataType, SchemaField, SchemaTypeStruct,
};
use file_store::Settings;
use futures::stream::{self, StreamExt};
use protobuf::CodedInputStream;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
pub use store::*;

use crate::{
    chunked_stream::ChunkedStream,
    proto::{get_delta_schema, get_descriptor, to_record_batch},
};

pub mod chunked_stream;
pub mod proto;
pub mod store;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Bucket name for the store. Required
    #[clap(long)]
    pub source_bucket: String,
    /// Optional api endpoint for the bucket. Default none
    #[clap(long)]
    pub source_endpoint: Option<String>,
    /// Optional region for the endpoint. Default: us-west-2
    #[clap(long)]
    pub source_region: String,

    /// Should only be used for local testing
    #[clap(long)]
    pub source_access_key_id: Option<String>,
    #[clap(long)]
    pub source_secret_access_key: Option<String>,
    #[clap(flatten)]
    pub source_filter: FileFilter,

    /// The base url of the proto definitions. Should be the proto directory
    #[clap(long)]
    pub source_proto_base_url: String,
    /// Path of the proto definition
    #[clap(long)]
    pub source_protos: Vec<String>,
    /// Name of the proto message
    #[clap(long)]
    pub source_proto_name: String,

    /// If provided, will use this column to partition by date
    #[clap(long)]
    pub partition_timestamp_column: Option<String>,

    #[clap(long)]
    pub target_bucket: String,
    #[clap(long)]
    pub target_table: String,
    /// Optional api endpoint for the bucket. Default none
    #[clap(long)]
    pub target_endpoint: Option<String>,
    /// Optional region for the endpoint. Default: us-west-2
    #[clap(long)]
    pub target_region: String,

    /// Should only be used for local testing
    #[clap(long)]
    pub target_access_key_id: Option<String>,
    #[clap(long)]
    pub target_secret_access_key: Option<String>,

    /// How large a batch to read in bytes
    #[clap(long, default_value = "500000000")]
    pub batch_size: i64,

    /// How many recoerds to decode at a time. Has an effect on memory pressure
    #[clap(long, default_value = "20000")]
    pub max_records: usize,
}

fn create_session(table: DeltaTable) -> Result<SessionContext> {
    let session = SessionContext::new();
    session.register_table("proto_table", Arc::new(table))?;

    Ok(session)
}

const ONE_DAY_MILLIS: i64 = 86_400_000;

#[tokio::main]
async fn main() -> Result<()> {
    use clap::Parser;
    let args = Args::parse();

    let descriptor = &get_descriptor(
        args.source_proto_base_url,
        args.source_protos,
        args.source_proto_name,
    )
    .await?;
    let mut delta_fields = get_delta_schema(&descriptor);
    if args.partition_timestamp_column.is_some() {
        let date_field = SchemaField::new(
            "date".to_string(),
            SchemaDataType::primitive("date".to_string()),
            false,
            HashMap::new(),
        );
        delta_fields.push(date_field);
    }
    delta_fields.push(SchemaField::new(
        "file".to_string(),
        SchemaDataType::primitive("string".to_string()),
        false,
        HashMap::new(),
    ));

    let mut s3_config =
        HashMap::from([("aws_default_region".to_string(), args.target_region.clone())]);
    if let Some(aws_secret_key_id) = args.target_secret_access_key {
        s3_config.insert("aws_secret_access_key".to_string(), aws_secret_key_id);
        s3_config.insert("allow_http".to_string(), "true".to_string());
    }
    if let Some(access_key_id) = args.target_access_key_id {
        s3_config.insert("aws_access_key_id".to_string(), access_key_id);
    }
    if let Some(endpoint) = args.target_endpoint {
        s3_config.insert("aws_endpoint".to_string(), endpoint);
    }

    let uri = format!("s3://{}/{}", args.target_bucket, args.target_table);
    let mut table_raw = DeltaTableBuilder::from_uri(uri.clone())
        .with_storage_options(s3_config.clone())
        .build()?;

    let maybe_table = table_raw.load().await;

    let mut table = match maybe_table {
        Ok(_) => table_raw,
        Err(DeltaTableError::NotATable(e)) => {
            println!(
                "It doesn't look like our delta table has been created, {}",
                e
            );
            DeltaOps(table_raw.into())
                .create()
                .with_save_mode(SaveMode::Ignore)
                .with_columns(delta_fields.clone())
                .with_partition_columns(vec!["date"])
                .await?
        }
        Err(err) => Err(err)?,
    };
    let mut source_filter = args.source_filter;
    let mut delta_schema = table.get_schema().expect("No schema").clone();
    // HACK: Enable schema evolution by adding a MetaData to the delta log
    // TODO: Remove this when https://github.com/delta-io/delta-rs/issues/1386 resolves
    if delta_schema.get_fields().len() < delta_fields.len()
        || delta_schema
            .get_fields()
            .iter()
            .enumerate()
            .any(|(i, f)| *f != delta_fields[i])
    {
        println!("New columns added");
        let metadata = DeltaTableMetaData::new(
            None,
            None,
            None,
            SchemaTypeStruct::new(delta_fields),
            vec!["date".to_string()],
            HashMap::new(),
        );
        let meta = action::MetaData::try_from(metadata)?;
        let actions = vec![Action::metaData(meta)];
        let storage = table.object_store();
        commit(storage.as_ref(), &actions, &table.state).await?;
        table.load().await?;
        delta_schema = table.get_schema().expect("No schema").clone();
    }

    // If no after is provided, process from 1 day before the last file, excluding files we already processed. This guarentees
    // that we catch any out-of-order files.
    if source_filter.after.is_none() && args.partition_timestamp_column.is_some() {
        let mut read_table = DeltaTableBuilder::from_uri(uri)
            .with_storage_options(s3_config)
            .build()?;
        read_table.load().await.expect("Failed to load");
        let session = create_session(read_table)?;
        let last_file_batches = session
            .sql("SELECT MAX(file) FROM proto_table")
            .await?
            .collect()
            .await?;
        if let Some(last_file_batch) = last_file_batches.get(0) {
            let last_file = last_file_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("Result is not a string array")?
                .value(0);
            if last_file != "" {
                println!("Processing from last file {}", last_file);
                let bucket_prefix = format!("s3://{}/", args.source_bucket);
                let full_prefix = format!("{}{}.", bucket_prefix, source_filter.file_prefix);
                let stripped_prefix = last_file.replace(full_prefix.as_str(), "");
                let timestamp_millis = stripped_prefix.split(".").next();
                let millis: i64 = timestamp_millis.context("No timestamp on file")?.parse()?;
                let one_day_ago_millis = millis - ONE_DAY_MILLIS;
                let datetime = NaiveDateTime::from_timestamp_millis(one_day_ago_millis)
                    .context("Could not construct date")?;

                // TODO: Change cargo.toml when this is fixed https://github.com/delta-io/delta-rs/issues/1445
                let query = format!(
                    "SELECT file FROM proto_table WHERE date >= '{}'",
                    datetime.format("%Y-%m-%d")
                );
                let batches = session.sql(query.as_str()).await?.collect().await?;
                let files_last_day = batches
                    .iter()
                    .map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .context("Result is not a string array")
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .flatten()
                    .map(|s| s.replace(bucket_prefix.as_str(), "").to_string())
                    .collect::<HashSet<_>>();

                // Take one millisecond after the last file
                source_filter.after = Some(datetime);
                source_filter.exclude = files_last_day;
            }
        }
    }

    let mut writer = RecordBatchWriter::for_table(&table)?;

    let file_store = AwsStore::from_settings(&Settings {
        bucket: args.source_bucket.clone(),
        endpoint: args.source_endpoint,
        region: args.source_region,
        access_key_id: args.source_access_key_id,
        secret_access_key: args.source_secret_access_key,
    })
    .await;

    let file_infos = file_store.list(source_filter).fuse();
    let mut chunked = ChunkedStream::new(file_infos, args.batch_size);

    while let Some(file_list) = chunked.next().await {
        let file_stream = file_store.source(Box::pin(stream::iter(file_list.into_iter())));
        // Chunk max records at a time
        let mut chunked_bytes = file_stream.chunks(args.max_records);
        while let Some(bytes) = chunked_bytes.next().await {
            let messages = bytes
                .into_iter()
                .map(|result| {
                    result.and_then(|(file, bytes)| {
                        Ok((
                            format!("s3://{}/{}", args.source_bucket, file.key),
                            descriptor
                                .parse_from(&mut CodedInputStream::from_bytes(bytes.as_ref()))?,
                        ))
                    })
                })
                // Explicitly ignore errors. They will be logged. Some historical data is malformed
                // https://discord.com/channels/837419358476304406/1017863042555461743/1117949837976018974
                .flatten()
                .collect::<Vec<_>>();

            let batch = to_record_batch(
                &delta_schema,
                descriptor.clone(),
                messages
                    .iter()
                    .map(|m| (m.0.as_str(), m.1.as_ref()))
                    .collect(),
                args.partition_timestamp_column.clone(),
            )?;

            writer.write(batch).await?;
        }
        println!("Flushing and committing");
        writer.flush_and_commit(&mut table).await?;
    }

    Ok(())
}

/** Crate private methods copied from delta rust/src/operations/transaction/mod.rs */

fn get_commit_bytes(actions: &Vec<Action>) -> Result<bytes::Bytes, TransactionError> {
    let mut extra_info = serde_json::Map::<String, Value>::new();
    let mut commit_info: CommitInfo = Default::default();
    commit_info.timestamp = Some(Utc::now().timestamp_millis());
    extra_info.insert(
        "clientVersion".to_string(),
        Value::String(format!("delta-rs.{}", crate_version())),
    );
    commit_info.info = extra_info;
    Ok(bytes::Bytes::from(log_entry_from_actions(
        actions
            .iter()
            .chain(std::iter::once(&Action::commitInfo(commit_info))),
    )?))
}

fn log_entry_from_actions<'a>(
    actions: impl IntoIterator<Item = &'a Action>,
) -> Result<String, TransactionError> {
    let mut jsons = Vec::<String>::new();
    for action in actions {
        let json = serde_json::to_string(action)
            .map_err(|e| TransactionError::SerializeLogJson { json_err: e })?;
        jsons.push(json);
    }
    Ok(jsons.join("\n"))
}

const DELTA_LOG_FOLDER: &str = "_delta_log";

/// Low-level transaction API. Creates a temporary commit file. Once created,
/// the transaction object could be dropped and the actual commit could be executed
/// with `DeltaTable.try_commit_transaction`.
pub(crate) async fn prepare_commit<'a>(
    storage: &dyn ObjectStore,
    actions: &Vec<Action>,
) -> Result<Path, TransactionError> {
    // Serialize all actions that are part of this log entry.
    let log_entry = get_commit_bytes(actions)?;

    // Write delta log entry as temporary file to storage. For the actual commit,
    // the temporary file is moved (atomic rename) to the delta log folder within `commit` function.
    let token = uuid::Uuid::new_v4().to_string();
    let file_name = format!("_commit_{token}.json.tmp");
    let path = Path::from_iter([DELTA_LOG_FOLDER, &file_name]);
    storage.put(&path, log_entry).await?;

    Ok(path)
}

/// Commit a transaction, with up to 5 retries. This is low-level transaction API.
///
/// Will error early if the a concurrent transaction has already been committed
/// and conflicts with this transaction.
pub async fn commit(
    storage: &dyn ObjectStore,
    actions: &Vec<Action>,
    read_snapshot: &DeltaTableState,
) -> Result<i64> {
    let tmp_commit = prepare_commit(storage, actions).await?;
    let version = read_snapshot.version() + 1;

    try_commit_transaction(storage, &tmp_commit, version)
        .await
        .map_err(|e| e.into())
}

fn commit_uri_from_version(version: i64) -> Path {
    let version = format!("{version:020}.json");
    Path::from(DELTA_LOG_FOLDER).child(version.as_str())
}

/// Tries to commit a prepared commit file. Returns [DeltaTableError::VersionAlreadyExists]
/// if the given `version` already exists. The caller should handle the retry logic itself.
/// This is low-level transaction API. If user does not want to maintain the commit loop then
/// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
/// with retry logic.
async fn try_commit_transaction(
    storage: &dyn ObjectStore,
    tmp_commit: &Path,
    version: i64,
) -> Result<i64> {
    // move temporary commit file to delta log directory
    // rely on storage to fail if the file already exists -
    storage
        .rename_if_not_exists(tmp_commit, &commit_uri_from_version(version))
        .await
        .map_err(|err| anyhow!("Failed to commit {:?}", err))?;
    Ok(version)
}
