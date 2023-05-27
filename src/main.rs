use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::Parser;
use deltalake::{
    arrow::ipc::RecordBatch,
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps, DeltaTableBuilder, DeltaTableError, SchemaDataType, SchemaField,
};
use file_store::{
    file_source, iot_beacon_report::IotBeaconReport, FileInfoStream, FileStore, FileType, Settings,
    Stream,
};
use futures::stream::{self, StreamExt};
use helium_proto::services::poc_lora::LoraBeaconIngestReportV1;
use protobuf::CodedInputStream;
pub use store::*;

use crate::proto::{get_delta_schema, get_descriptor, to_record_batch};

pub mod error;
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
}

#[tokio::main]
async fn main() {
    use clap::Parser;
    let args = Args::parse();

    let descriptor = get_descriptor(args.source_protos, args.source_proto_name).await;
    let delta_schema = get_delta_schema(
        descriptor.clone(),
        args.partition_timestamp_column.is_some(),
    );

    let file_store = AwsStore::from_settings(&Settings {
        bucket: args.source_bucket.clone(),
        endpoint: args.source_endpoint,
        region: args.source_region,
        access_key_id: args.source_access_key_id,
        secret_access_key: args.source_secret_access_key,
    });

    let file_infos = file_store.list(args.source_filter);

    let file_stream = file_store.source(file_infos);

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
    let mut table_raw = DeltaTableBuilder::from_uri(uri)
        .with_storage_options(s3_config)
        .build()
        .unwrap();

    let maybe_table = table_raw.load().await;

    let mut table = match maybe_table {
        Ok(_) => table_raw,
        Err(DeltaTableError::NotATable(_)) => {
            println!("It doesn't look like our delta table has been created");
            DeltaOps(table_raw.into())
                .create()
                .with_columns(delta_schema)
                .with_partition_columns(vec!["date"])
                .await
                .unwrap()
        }
        Err(err) => Err(err).unwrap(),
    };

    let mut writer =
        RecordBatchWriter::for_table(&table).expect("Failed to make RecordBatchWriter");

    let records_bytes = file_stream.collect::<Vec<_>>().await;
    let messages = records_bytes
        .iter()
        .map(|result| {
            let bytes = result.as_ref().expect("Failed to get bytes");
            descriptor
                .parse_from(&mut CodedInputStream::from_bytes(bytes.as_ref()))
                .expect("Failed to decode message")
        })
        .collect::<Vec<_>>();
    let batch = to_record_batch(
        table.get_schema().expect("No schema"),
        descriptor,
        messages.iter().map(|m| m.as_ref()).collect(),
        args.partition_timestamp_column,
    );

    writer.write(batch).await.expect("Failed to write");

    writer
        .flush_and_commit(&mut table)
        .await
        .expect("Failed to flush write");
}

// #[derive(Clone, Debug)]
// pub struct IotBeaconReport {
//     pub pub_key: PublicKeyBinary,
//     pub local_entropy: Vec<u8>,
//     pub remote_entropy: Vec<u8>,
//     pub data: Vec<u8>,
//     pub frequency: u64,
//     pub channel: i32,
//     pub datarate: DataRate,
//     pub tx_power: i32,
//     pub timestamp: DateTime<Utc>,
//     pub signature: Vec<u8>,
//     pub tmst: u32,
// }

// fn file_type_to_schema(file_type: FileType) -> Vec<SchemaField> {
//     match file_type {
//         FileType::IotBeaconIngestReport => {
//             LoraBeaconIngestReportV1::default().descriptor();

//             vec![
//                 SchemaField::new(
//                     "received_timestamp".to_string(),
//                     SchemaDataType::primitive("timestamp".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "pubkey".to_string(),
//                     SchemaDataType::primitive("binary".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "local_entropy".to_string(),
//                     SchemaDataType::primitive("binary".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "remote_entropy".to_string(),
//                     SchemaDataType::primitive("binary".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "data".to_string(),
//                     SchemaDataType::primitive("binary".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "frequency".to_string(),
//                     SchemaDataType::primitive("decimal(23,0)".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "channel".to_string(),
//                     SchemaDataType::primitive("integer".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "datarate".to_string(),
//                     SchemaDataType::primitive("integer".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "tx_power".to_string(),
//                     SchemaDataType::primitive("integer".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "timestamp".to_string(),
//                     SchemaDataType::primitive("timestamp".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "signature".to_string(),
//                     SchemaDataType::primitive("binary".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//                 SchemaField::new(
//                     "tmst".to_string(),
//                     SchemaDataType::primitive("integer".to_string()),
//                     true,
//                     HashMap::new(),
//                 ),
//             ]
//         }
//         _ => vec![],
//     }
// }
