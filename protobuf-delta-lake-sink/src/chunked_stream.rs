use anyhow::Error;
use futures::ready;
use futures::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct ChunkedStream<S, T, F>
where
    S: Stream<Item = Result<T, Error>> + Unpin,
    F: FnMut(&T) -> i64
{
    stream: S,
    chunk_size: i64,
    chunk: Vec<Result<T, Error>>,
    current_size: i64,
    func: F,
}

impl<S, T, F> ChunkedStream<S, T, F>
where
    S: Stream<Item = Result<T, Error>> + Unpin,
    F: FnMut(&T) -> i64,
{
    pub fn new(stream: S, chunk_size: i64, chunk_fn: F) -> Self {
        ChunkedStream {
            stream,
            chunk_size,
            chunk: vec![],
            current_size: 0,
            func: chunk_fn,
        }
    }
}

impl<S, T, F> Stream for ChunkedStream<S, T, F>
where
    S: Stream<Item = Result<T, Error>> + Unpin,
    F: FnMut(&T) -> i64,
{
    type Item = Vec<Result<T, Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = unsafe { self.get_unchecked_mut() };

        let mut leftover = None;
        while let Some(item) = ready!(Pin::new(&mut self_mut.stream).poll_next(cx)) {
            match item {
                Err(e) => self_mut.chunk.push(Err(e)),
                Ok(item) => {
                    let item_size = (self_mut.func)(&item);

                    if self_mut.current_size + item_size <= self_mut.chunk_size {
                        // Add item to the current chunk
                        self_mut.chunk.push(Ok(item));
                        self_mut.current_size += item_size;
                    } else {
                        leftover = Some(item);
                        break;
                    }
                }
            }
        }

        if !self_mut.chunk.is_empty() {
            let ret = std::mem::take(&mut self_mut.chunk);
            self_mut.current_size = 0;
            if let Some(left) = leftover {
              self_mut.chunk.push(Ok(left));
            }
            // Return the last remaining chunk
            Poll::Ready(Some(ret))
        } else {
          if let Some(left) = leftover {
            Poll::Ready(Some(vec![Ok(left)]))
          } else {
            // End of stream
            Poll::Ready(None)
          }
        }
    }
}
