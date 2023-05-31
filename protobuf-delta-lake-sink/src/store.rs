use bytes::BytesMut;
use file_store::Settings;
use lazy_static::lazy_static;
use regex::Regex;

use crate::error::{Error, Result};
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client, Config};
use chrono::NaiveDateTime;
use futures::{
    stream::{self, BoxStream, StreamExt},
    FutureExt, TryFutureExt, TryStreamExt,
};
use std::str::FromStr;

pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type BytesMutStream = Stream<BytesMut>;
pub type FileInfoStream = Stream<(String, i64)>;

#[derive(Debug, clap::Args)]
pub struct FileFilter {
    /// Optional start time to look for (inclusive). Defaults to the oldest
    /// timestamp in the bucket.
    #[clap(long)]
    after: Option<NaiveDateTime>,
    /// Optional end time to look for (exclusive). Defaults to the latest
    /// available timestamp in the bucket.
    #[clap(long)]
    before: Option<NaiveDateTime>,
    /// The file prefix to search for
    #[clap(long)]
    file_prefix: String,
}

lazy_static! {
    static ref RE: Regex = Regex::new(r"([a-z,_]+).(\d+)(.gz)?").unwrap();
}

pub fn client_from_settings(settings: &Settings) -> Client {
    // Configure AWS S3 client
    let region = Region::new(settings.region.clone());
    let mut config = Config::builder().region(region);
    config = match (
        settings.access_key_id.clone(),
        settings.secret_access_key.clone(),
    ) {
        (Some(access_key_id), Some(secret_access_key)) => {
            let credentials =
                aws_sdk_s3::config::Credentials::from_keys(access_key_id, secret_access_key, None);
            config.credentials_provider(credentials)
        }
        _ => config,
    };

    println!("Endpoint: {:?}", settings.endpoint.clone());
    config = match settings.endpoint.clone() {
        Some(endpoint) => {
            config.set_force_path_style(Some(true));
            config.endpoint_url(endpoint)
        }
        _ => config,
    };

    // Create AWS S3 client
    Client::from_conf(config.build())
}

pub struct AwsStore {
    pub bucket: String,
    pub client: Client,
}

impl AwsStore {
    pub fn from_settings(settings: &Settings) -> AwsStore {
        AwsStore {
            bucket: settings.bucket.clone(),
            client: client_from_settings(settings),
        }
    }

    pub fn list(&self, file_filter: FileFilter) -> Stream<(String, i64)> {
        let before = file_filter.before;
        let after = file_filter.after;
        let request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&file_filter.file_prefix)
            .set_start_after(
                file_filter
                    .after
                    .map(|dt| format!("{}.{}.gz", file_filter.file_prefix, dt.timestamp_millis())),
            );

        futures::stream::unfold(
            (request, true, None),
            |(req, first_time, next)| async move {
                if first_time || next.is_some() {
                    let list_objects_response =
                        req.clone().set_continuation_token(next).send().await;

                    let next_token = list_objects_response
                        .as_ref()
                        .ok()
                        .and_then(|r| r.next_continuation_token())
                        .map(|x| x.to_owned());

                    Some((list_objects_response, (req, false, next_token)))
                } else {
                    None
                }
            },
        )
        .flat_map(move |entry| match entry {
            Ok(output) => {
                let filtered = output
                    .contents
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|obj| {
                        let key = obj.key().unwrap_or_default();
                        if RE.is_match(key) {
                            let ts =
                                i64::from_str(key.split(".").nth(1).unwrap_or_default()).unwrap();

                            Some((key.to_string(), ts))
                        } else {
                            None
                        }
                    })
                    .filter(move |info| after.map_or(true, |v| info.1 > v.timestamp_millis()))
                    .filter(move |info| before.map_or(true, |v| info.1 <= v.timestamp_millis()))
                    .map(Ok);
                stream::iter(filtered).boxed()
            }
            Err(err) => {
                println!("Error: {:?}", err);
                stream::once(async move { Err(Error::s3_error(err)) }).boxed()
            }
        })
        .boxed()
    }

    /// Stream a series of ordered items from the store from remote files with
    /// the given keys.
    pub fn source(&self, infos: FileInfoStream) -> BytesMutStream {
        let bucket = self.bucket.clone();
        let client = self.client.clone();
        infos
            .map_ok(move |info| get_byte_stream(client.clone(), bucket.clone(), info.0))
            .try_buffered(2)
            .flat_map(|stream| match stream {
                Ok(stream) => stream_source(stream),
                Err(err) => stream::once(async move { Err(err) }).boxed(),
            })
            .fuse()
            .boxed()
    }
}

fn stream_source(stream: ByteStream) -> BytesMutStream {
    use async_compression::tokio::bufread::GzipDecoder;
    use tokio_util::{
        codec::{length_delimited::LengthDelimitedCodec, FramedRead},
        io::StreamReader,
    };

    Box::pin(
        FramedRead::new(
            GzipDecoder::new(StreamReader::new(stream)),
            LengthDelimitedCodec::new(),
        )
        .map_err(Error::from),
    )
}

async fn get_byte_stream<K>(client: Client, bucket: String, key: K) -> Result<ByteStream>
where
    K: Into<String>,
{
    client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .map_ok(|output| output.body)
        .map_err(Error::s3_error)
        .fuse()
        .await
}
