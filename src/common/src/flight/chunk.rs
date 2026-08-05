//! # Batch Chunking for Flight Transport
//!
//! Splits oversized `RecordBatch`es into pieces that each encode into a
//! FlightData message well below [`super::MAX_GRPC_MESSAGE_SIZE`].
//!
//! `batches_to_flight_data` turns one `RecordBatch` into one FlightData
//! message; a batch whose encoded size exceeds the receiver's gRPC limit
//! makes `do_put` fail no matter how often it is retried. The acceptor's
//! WAL retry loop would replay such an entry forever. Chunking at the
//! sender guarantees any batch (under the per-row size assumption) can
//! transit.

use datafusion::arrow::array::{MutableArrayData, make_array};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;

/// Per-chunk budget for the in-memory size of a `RecordBatch` destined for
/// a single FlightData message.
///
/// A quarter of [`super::MAX_GRPC_MESSAGE_SIZE`]: the in-memory size is an
/// estimate of the IPC-encoded size, so the 4x headroom absorbs encoding
/// overhead, protobuf framing, and rows whose sizes are skewed relative to
/// the batch average.
pub const MAX_ENCODED_BATCH_SIZE: usize = super::MAX_GRPC_MESSAGE_SIZE / 4;

/// Split a `RecordBatch` into chunks whose in-memory size each stays at or
/// below `max_chunk_bytes` (single-row chunks excepted — one row cannot be
/// split further).
///
/// Rows are preserved in order: concatenating the returned chunks yields
/// the original batch. Batches already under budget are returned as a
/// single (zero-copy) clone.
///
/// Chunks are compacted into their own buffers because `RecordBatch::slice`
/// shares the parent's buffers, which would make both the size estimate and
/// the IPC encoding of a raw slice reflect the full parent allocation.
///
/// # Errors
///
/// Returns an error only if Arrow fails to concatenate a slice into a
/// compact batch, which indicates schema inconsistency and cannot happen
/// for slices of a valid batch.
pub fn split_batch_for_grpc(
    batch: &RecordBatch,
    max_chunk_bytes: usize,
) -> Result<Vec<RecordBatch>, ArrowError> {
    if batch.num_rows() == 0 || batch.get_array_memory_size() <= max_chunk_bytes {
        return Ok(vec![batch.clone()]);
    }

    // First pass: cut by the average bytes-per-row. Skewed rows are handled
    // by the per-chunk verification below.
    let bytes_per_row = (batch.get_array_memory_size() / batch.num_rows()).max(1);
    let rows_per_chunk = (max_chunk_bytes / bytes_per_row).max(1);

    let mut chunks = Vec::with_capacity(batch.num_rows().div_ceil(rows_per_chunk));
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = rows_per_chunk.min(batch.num_rows() - offset);
        let chunk = compact(&batch.slice(offset, len))?;
        push_verified(chunk, max_chunk_bytes, &mut chunks)?;
        offset += len;
    }
    Ok(chunks)
}

/// Push a compacted chunk, halving it recursively while it still exceeds
/// the budget. A single row that exceeds the budget is emitted as-is: it
/// cannot be split further, and the budget's headroom below the gRPC limit
/// exists precisely to absorb such rows.
fn push_verified(
    chunk: RecordBatch,
    max_chunk_bytes: usize,
    out: &mut Vec<RecordBatch>,
) -> Result<(), ArrowError> {
    if chunk.num_rows() <= 1 || chunk.get_array_memory_size() <= max_chunk_bytes {
        out.push(chunk);
        return Ok(());
    }
    let half = chunk.num_rows() / 2;
    let rest = chunk.num_rows() - half;
    push_verified(compact(&chunk.slice(0, half))?, max_chunk_bytes, out)?;
    push_verified(compact(&chunk.slice(half, rest))?, max_chunk_bytes, out)
}

/// Copy a (possibly sliced) batch into buffers that hold exactly its rows.
///
/// `concat_batches` cannot be used here: arrow's `concat` short-circuits a
/// single input into a zero-copy slice, which keeps the parent's full
/// buffers alive — `get_array_memory_size` would still report them and the
/// IPC encoder could still write them. `MutableArrayData` copies exactly
/// the referenced range into fresh, tight buffers.
fn compact(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let data = column.to_data();
            let mut mutable = MutableArrayData::new(vec![&data], false, data.len());
            mutable.extend(0, 0, data.len());
            make_array(mutable.freeze())
        })
        .collect();
    RecordBatch::try_new(batch.schema(), columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Build a batch of `rows` rows, each carrying a `payload_bytes`-sized
    /// string plus an i64 sequence number identifying the row.
    fn synthetic_batch(rows: usize, payload_bytes: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let seq: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
        let payload: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..rows).map(|_| "x".repeat(payload_bytes)),
        ));
        RecordBatch::try_new(schema, vec![seq, payload]).expect("valid batch")
    }

    fn sequence_numbers(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("seq column")
            .values()
            .to_vec()
    }

    #[test]
    fn small_batch_is_returned_unsplit() {
        let batch = synthetic_batch(100, 10);
        let budget = 1024 * 1024;
        assert!(batch.get_array_memory_size() <= budget);

        let chunks = split_batch_for_grpc(&batch, budget).expect("split");

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), batch.num_rows());
    }

    #[test]
    fn empty_batch_is_returned_unsplit() {
        let batch = synthetic_batch(0, 0);
        let chunks = split_batch_for_grpc(&batch, 1024).expect("split");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 0);
    }

    #[test]
    fn large_batch_chunks_all_stay_under_budget() {
        // ~4 MiB of uniform rows against a 256 KiB budget.
        let batch = synthetic_batch(4096, 1024);
        let budget = 256 * 1024;
        assert!(batch.get_array_memory_size() > budget);

        let chunks = split_batch_for_grpc(&batch, budget).expect("split");

        assert!(chunks.len() > 1, "oversized batch must be split");
        for (i, chunk) in chunks.iter().enumerate() {
            assert!(chunk.num_rows() > 0, "chunk {i} must not be empty");
            assert!(
                chunk.get_array_memory_size() <= budget,
                "chunk {i} is {} bytes, budget is {budget}",
                chunk.get_array_memory_size(),
            );
        }
    }

    #[test]
    fn chunks_preserve_all_rows_in_order_and_concatenate_back() {
        let batch = synthetic_batch(4096, 1024);
        let budget = 256 * 1024;

        let chunks = split_batch_for_grpc(&batch, budget).expect("split");

        let total_rows: usize = chunks.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, batch.num_rows());

        let reassembled =
            concat_batches(&batch.schema(), chunks.iter()).expect("chunks must concatenate");
        assert_eq!(reassembled.num_rows(), batch.num_rows());
        assert_eq!(sequence_numbers(&reassembled), sequence_numbers(&batch));
    }

    #[test]
    fn skewed_rows_are_halved_until_under_budget() {
        // One giant row among small ones breaks the average-bytes-per-row
        // estimate; the verification pass must halve the offending chunk.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        let budget = 64 * 1024;
        let mut values: Vec<String> = (0..1024).map(|_| "y".repeat(16)).collect();
        values[512] = "z".repeat(4 * budget);
        let payload: ArrayRef = Arc::new(StringArray::from_iter_values(values.iter()));
        let batch = RecordBatch::try_new(schema, vec![payload]).expect("valid batch");

        let chunks = split_batch_for_grpc(&batch, budget).expect("split");

        let total_rows: usize = chunks.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, batch.num_rows());
        for chunk in &chunks {
            assert!(
                chunk.num_rows() == 1 || chunk.get_array_memory_size() <= budget,
                "multi-row chunk over budget: {} rows, {} bytes",
                chunk.num_rows(),
                chunk.get_array_memory_size(),
            );
        }
    }

    #[test]
    fn single_oversized_row_is_emitted_rather_than_looping() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        let budget = 1024;
        let payload: ArrayRef = Arc::new(StringArray::from_iter_values(["w".repeat(16 * budget)]));
        let batch = RecordBatch::try_new(schema, vec![payload]).expect("valid batch");

        let chunks = split_batch_for_grpc(&batch, budget).expect("split");

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 1);
    }

    /// The per-chunk budget must leave the promised 4x headroom below the
    /// transport limit so estimate error can never produce a message the
    /// receiver rejects.
    #[test]
    fn encoded_batch_budget_leaves_headroom_under_grpc_limit() {
        assert_eq!(
            MAX_ENCODED_BATCH_SIZE * 4,
            crate::flight::MAX_GRPC_MESSAGE_SIZE
        );
    }

    /// End-to-end property the WAL retry loop depends on: the in-memory
    /// budget bounds the *encoded* FlightData size within the headroom
    /// that [`MAX_ENCODED_BATCH_SIZE`] reserves below the gRPC limit.
    #[test]
    fn flight_data_messages_from_chunks_track_the_budget() {
        let batch = synthetic_batch(8192, 1024); // ~8 MiB
        let budget = 1024 * 1024;
        let chunks = split_batch_for_grpc(&batch, budget).expect("split");
        assert!(chunks.len() > 1, "batch must be split for this test");

        let flight_data =
            arrow_flight::utils::batches_to_flight_data(&batch.schema(), chunks).expect("encode");

        // First message carries the schema; the rest carry one chunk each.
        for (i, message) in flight_data.iter().enumerate().skip(1) {
            let encoded =
                message.data_header.len() + message.data_body.len() + message.app_metadata.len();
            assert!(
                encoded < budget * 2,
                "FlightData message {i} is {encoded} bytes; encoding must stay \
                 within 2x of the {budget}-byte in-memory budget (the shared \
                 constant reserves 4x headroom below the gRPC limit)",
            );
        }
    }
}
