use anyhow::{Context, Result};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::NaiveDateTime;
use clap::Parser;
use datafusion::arrow::array::StringArray;
use deltalake::{
    datafusion::prelude::SessionContext,
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps, DeltaTable, DeltaTableBuilder, DeltaTableError, SchemaDataType, SchemaField,
};
use file_store::Settings;
use futures::stream::StreamExt;
use protobuf::CodedInputStream;
pub use store::*;

use crate::proto::{get_delta_schema, get_descriptor, to_record_batch};

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

    /// Url of the proto definition
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

    /// Maximum number of records to write at a time. Controls the size of outputted parquet files, and memory usage.
    #[clap(long, default_value = "1000000")]
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

    let descriptor = &get_descriptor(args.source_protos, args.source_proto_name).await?;
    let mut delta_schema = get_delta_schema(&descriptor);
    if args.partition_timestamp_column.is_some() {
        let date_field = SchemaField::new(
            "date".to_string(),
            SchemaDataType::primitive("date".to_string()),
            false,
            HashMap::new(),
        );
        delta_schema.push(date_field);
    }
    delta_schema.push(SchemaField::new(
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
        Err(DeltaTableError::NotATable(_)) => {
            println!("It doesn't look like our delta table has been created");
            DeltaOps(table_raw.into())
                .create()
                .with_columns(delta_schema)
                .with_partition_columns(vec!["date"])
                .await?
        }
        Err(err) => Err(err)?,
    };
    let mut source_filter = args.source_filter;
    let delta_schema = table.get_schema().expect("No schema");
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

            println!("Processing from last file {}", last_file);
            let bucket_prefix = format!("s3://{}/", args.source_bucket);
            let full_prefix = format!("{}{}.", bucket_prefix, source_filter.file_prefix);
            let stripped_prefix = last_file.replace(full_prefix.as_str(), "");
            let timestamp_millis = stripped_prefix.split(".").next();
            let millis: i64 = timestamp_millis.context("No timestamp on file")?.parse()?;
            let one_day_ago_millis = millis - ONE_DAY_MILLIS;
            let datetime = NaiveDateTime::from_timestamp_millis(one_day_ago_millis).context("Could not construct date")?;

            // TODO: Uncomment this when this is fixed https://github.com/delta-io/delta-rs/issues/1445
            // let query = format!(
            //   "SELECT file FROM proto_table WHERE {} >= {}", 
            //   args.partition_timestamp_column.clone().unwrap(),
            //   one_day_ago_millis
            // );
            let query = format!(
              "SELECT file FROM proto_table WHERE date >= '{}'", 
              datetime.format("%Y-%m-%d")
            );
            println!("Query {}", query);
            let batches = session
            .sql(query.as_str())
            .await?
            .collect()
            .await?;
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

            println!("Files last day {:?}", files_last_day);

            // Take one millisecond after the last file
            source_filter.after = Some(datetime);
            source_filter.exclude = files_last_day;
        }
    }

    let mut writer = RecordBatchWriter::for_table(&table)?;

    let file_store = AwsStore::from_settings(&Settings {
        bucket: args.source_bucket.clone(),
        endpoint: args.source_endpoint,
        region: args.source_region,
        access_key_id: args.source_access_key_id,
        secret_access_key: args.source_secret_access_key,
    });

    let file_infos = file_store.list(source_filter);
    let file_stream = file_store.source(file_infos);

    let mut chunked_stream = file_stream.chunks(args.max_records);
    while let Some(item) = chunked_stream.next().await {
        let messages = item
            .into_iter()
            .map(|result| {
                result.and_then(|(file, bytes)| {
                    Ok((
                        format!("s3://{}/{}", args.source_bucket, file.key),
                        descriptor.parse_from(&mut CodedInputStream::from_bytes(bytes.as_ref()))?,
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = to_record_batch(
            delta_schema,
            descriptor.clone(),
            messages
                .iter()
                .map(|m| (m.0.as_str(), m.1.as_ref()))
                .collect(),
            args.partition_timestamp_column.clone(),
        )?;

        writer.write(batch).await?
    }

    writer.flush_and_commit(&mut table).await?;

    Ok(())
}
