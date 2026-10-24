use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};
use rdf_fusion_common::DFResult;
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncRead;

/// Opens a reader for all lines of an object that *start* within `range`.
///
/// Splitting an object into consecutive ranges and opening a reader for each range yields every
/// line exactly once. This allows parsing line-based formats (e.g., N-Triples) in parallel.
///
/// The reader starts reading at the beginning of the range and stops after the line that
/// contains the last byte of the range. Therefore, the reader may read beyond the range.
pub async fn open_line_range_reader(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    range: Range<u64>,
) -> DFResult<Box<dyn AsyncRead + Unpin + Send + 'static>> {
    // For all ranges except the first one, start reading at the byte before the range. The
    // (partial) line up to and including the first line break belongs to the previous range.
    let read_start = range.start.saturating_sub(1);
    let options = GetOptions {
        range: Some(GetRange::Offset(read_start)),
        ..Default::default()
    };
    let stream = store.get_opts(path, options).await?.into_stream();
    let stream = line_range_stream(stream, read_start, range.start > 0, range.end)
        .fuse()
        .boxed();
    Ok(Box::new(tokio_util::io::StreamReader::new(stream)))
}

/// The state of [`line_range_stream`].
struct LineRangeState {
    inner: BoxStream<'static, object_store::Result<Bytes>>,
    /// The position of the next byte in `inner`.
    position: u64,
    /// Whether the bytes up to and including the next line break should be skipped.
    skip_first_line: bool,
    /// The end of the range.
    end: u64,
    /// Whether the last line has been emitted.
    done: bool,
}

/// Restricts `inner`, which starts at `position`, to the lines that start before `end`.
fn line_range_stream(
    inner: BoxStream<'static, object_store::Result<Bytes>>,
    position: u64,
    skip_first_line: bool,
    end: u64,
) -> impl Stream<Item = std::io::Result<Bytes>> + Send + 'static {
    let state = LineRangeState {
        inner,
        position,
        skip_first_line,
        end,
        done: false,
    };
    futures::stream::unfold(state, |mut state| async move {
        loop {
            if state.done {
                return None;
            }

            let mut chunk = match state.inner.next().await? {
                Ok(chunk) => chunk,
                Err(err) => {
                    state.done = true;
                    return Some((Err(std::io::Error::other(err)), state));
                }
            };
            let mut chunk_start = state.position;
            state.position += chunk.len() as u64;

            if state.skip_first_line {
                let Some(line_break) = find_line_break(&chunk, 0) else {
                    continue;
                };
                chunk = chunk.slice(line_break + 1..);
                chunk_start += line_break as u64 + 1;
                state.skip_first_line = false;

                // The line after the skipped bytes starts after the range. Hence, it belongs to
                // the next range.
                if chunk_start >= state.end {
                    return None;
                }
            }

            // The last line of the range is the line that contains the last byte of the range.
            // Once this byte is in the chunk, the range ends with the next line break.
            let last_byte = state.end - 1;
            let search_start = last_byte.saturating_sub(chunk_start);
            if search_start < chunk.len() as u64
                && let Some(line_break) = find_line_break(&chunk, search_start as usize)
            {
                chunk = chunk.slice(..line_break + 1);
                state.done = true;
            }

            if !chunk.is_empty() {
                return Some((Ok(chunk), state));
            }
        }
    })
}

/// Returns the index of the first line break in `bytes` at or after `from`.
fn find_line_break(bytes: &[u8], from: usize) -> Option<usize> {
    bytes[from..]
        .iter()
        .position(|b| *b == b'\n')
        .map(|i| from + i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};
    use tokio::io::AsyncReadExt;

    async fn read_all_ranges(data: &'static str, num_ranges: u64) -> Vec<String> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("data.nt");
        store
            .put(&path, PutPayload::from_static(data.as_bytes()))
            .await
            .unwrap();

        let size = data.len() as u64;
        let range_size = size.div_ceil(num_ranges);
        let mut result = Vec::new();
        for i in 0..num_ranges {
            let range = (i * range_size).min(size)..((i + 1) * range_size).min(size);
            if range.is_empty() {
                continue;
            }
            let mut reader = open_line_range_reader(Arc::clone(&store), &path, range)
                .await
                .unwrap();
            let mut content = String::new();
            reader.read_to_string(&mut content).await.unwrap();
            result.push(content);
        }
        result
    }

    #[tokio::test]
    async fn every_line_is_read_exactly_once() {
        let data = "a\nbb\n\nccc\ndddd\ne\nfffffffffff\ng";
        for num_ranges in 1..=(data.len() as u64) {
            let ranges = read_all_ranges(data, num_ranges).await;
            assert_eq!(ranges.concat(), data, "{num_ranges} ranges: {ranges:?}");
            for range in &ranges[..ranges.len() - 1] {
                assert!(range.is_empty() || range.ends_with('\n'), "{ranges:?}");
            }
        }
    }

    #[tokio::test]
    async fn every_line_is_read_exactly_once_with_small_chunks() {
        let data = "a\nbb\n\nccc\ndddd\ne\nfffffffffff\ng\n";
        let size = data.len() as u64;
        for chunk_size in 1..=4 {
            for num_ranges in 1..=size {
                let range_size = size.div_ceil(num_ranges);
                let mut result = String::new();
                for i in 0..num_ranges {
                    let start = (i * range_size).min(size);
                    let end = ((i + 1) * range_size).min(size);
                    if start >= end {
                        continue;
                    }
                    let read_start = start.saturating_sub(1);
                    let chunks = data.as_bytes()[read_start as usize..]
                        .chunks(chunk_size)
                        .map(|c| Ok(Bytes::copy_from_slice(c)))
                        .collect::<Vec<_>>();
                    let stream = line_range_stream(
                        futures::stream::iter(chunks).boxed(),
                        read_start,
                        start > 0,
                        end,
                    );
                    let bytes = stream
                        .map(|b| b.unwrap())
                        .collect::<Vec<_>>()
                        .await
                        .concat();
                    result.push_str(std::str::from_utf8(&bytes).unwrap());
                }
                assert_eq!(result, data, "chunk size {chunk_size}, {num_ranges} ranges");
            }
        }
    }
}
