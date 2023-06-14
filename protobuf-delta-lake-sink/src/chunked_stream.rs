use anyhow::Error;
use futures::ready;
use futures::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::FileInfo;

pub struct ChunkedStream<S>
where
    S: Stream<Item = Result<FileInfo, Error>> + Unpin,
{
    stream: S,
    chunk_size: i64,
    leftover: Option<FileInfo>,
}

impl<S> ChunkedStream<S>
where
    S: Stream<Item = Result<FileInfo, Error>> + Unpin,
{
    pub fn new(stream: S, chunk_size: i64) -> Self {
        ChunkedStream {
            stream,
            chunk_size,
            leftover: None,
        }
    }
}

impl<S> Stream for ChunkedStream<S>
where
    S: Stream<Item = Result<FileInfo, Error>> + Unpin,
{
    type Item = Vec<Result<FileInfo, Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut chunk = Vec::new();
        let mut current_size = 0;
        if let Some(file_info) = &self.leftover {
            current_size = file_info.size;
            chunk.push(Ok(file_info.clone()));
        }

        while let Some(item) = ready!(Pin::new(&mut self.stream).poll_next(cx)) {
            match item {
                Err(e) => chunk.push(Err(e)),
                Ok(file_info) => {
                    let item_size = file_info.size;

                    if current_size + item_size <= self.chunk_size {
                        // Add item to the current chunk
                        chunk.push(Ok(file_info));
                        current_size += item_size;
                    } else {
                        self.leftover = Some(file_info);
                        break;
                    }
                }
            }
        }

        if !chunk.is_empty() {
            // Return the last remaining chunk
            Poll::Ready(Some(chunk))
        } else {
            // End of stream
            Poll::Ready(None)
        }
    }
}
