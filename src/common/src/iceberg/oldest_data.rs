//! The earliest event timestamp a signal table holds, read from Iceberg
//! metadata only: the manifest list's partition summaries pick the oldest
//! hour, and the data files of that hour supply their column lower bounds.
//! No data file is ever opened.

use anyhow::{Context, Result};
use futures::StreamExt;
use iceberg_rust::spec::manifest::{DataFile, Status};
use iceberg_rust::spec::manifest_list::{Content, ManifestListEntry};
use iceberg_rust::spec::partition::Transform;
use iceberg_rust::spec::types::{PrimitiveType, Type};
use iceberg_rust::spec::values::Value;
use iceberg_rust::table::Table;

const NANOS_PER_HOUR: i64 = 3_600 * 1_000_000_000;

/// The earliest timestamp, in unix nanoseconds, among data files given as
/// `(partition_hour, column_lower_bound_ns)`.
///
/// Only the lowest partition hour can hold the earliest event. Within it, the
/// smallest column lower bound is the answer, unless some file of that hour
/// carries no bound: that file could hold anything in the hour, so the hour's
/// floor is the only safe answer then. `None` for no files at all.
pub fn earliest_event_ns(files: impl IntoIterator<Item = (i64, Option<i64>)>) -> Option<i64> {
    // (hour, smallest bound in that hour, whether every file had one)
    let mut best: Option<(i64, i64, bool)> = None;
    for (hour, bound) in files {
        match &mut best {
            Some((best_hour, _, _)) if hour > *best_hour => {}
            Some((best_hour, min_bound, all_bounded)) if hour == *best_hour => match bound {
                Some(bound) => *min_bound = (*min_bound).min(bound),
                None => *all_bounded = false,
            },
            _ => best = Some((hour, bound.unwrap_or(i64::MAX), bound.is_some())),
        }
    }
    let (hour, min_bound, all_bounded) = best?;
    let floor = hour.checked_mul(NANOS_PER_HOUR)?;
    Some(if all_bounded {
        min_bound.max(floor)
    } else {
        floor
    })
}

/// The hour partition of a table: its partition field name and position in
/// the spec, and the field id and type of the column it is derived from.
struct HourPartition {
    name: String,
    position: usize,
    source_id: i32,
    source_type: Type,
}

fn hour_partition(table: &Table) -> Result<Option<HourPartition>> {
    let metadata = table.metadata();
    let spec = metadata
        .default_partition_spec()
        .context("table has no default partition spec")?;
    let Some((position, field)) = spec
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| matches!(field.transform(), Transform::Hour))
    else {
        return Ok(None);
    };
    let schema = metadata
        .current_schema()
        .context("table has no current schema")?;
    let Some(source) = schema.get(*field.source_id() as usize) else {
        return Ok(None);
    };
    Ok(Some(HourPartition {
        name: field.name().clone(),
        position,
        source_id: *field.source_id(),
        source_type: source.field_type.clone(),
    }))
}

/// A column bound as unix nanoseconds. Iceberg `timestamp`/`timestamptz` are
/// microseconds; the `_ns` variants and a `long` column (an `*_unix_nano`
/// value) are nanoseconds.
fn bound_ns(value: &Value, column_type: &Type) -> Option<i64> {
    match (value, column_type) {
        (
            Value::Timestamp(v) | Value::TimestampTZ(v),
            Type::Primitive(PrimitiveType::Timestamp | PrimitiveType::Timestamptz),
        ) => v.checked_mul(1_000),
        (
            Value::Timestamp(v) | Value::TimestampTZ(v) | Value::LongInt(v),
            Type::Primitive(
                PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs | PrimitiveType::Long,
            ),
        ) => Some(*v),
        _ => None,
    }
}

fn int_value(value: &Value) -> Option<i64> {
    match value {
        Value::Int(v) => Some(i64::from(*v)),
        Value::LongInt(v) => Some(*v),
        _ => None,
    }
}

/// The lower bound of the hour partition across a manifest's files, from the
/// manifest list's partition summary. `None` when the manifest carries no
/// usable summary, so it has to be read.
fn manifest_min_hour(manifest: &ManifestListEntry, position: usize) -> Option<i64> {
    manifest
        .partitions
        .as_ref()?
        .get(position)?
        .lower_bound
        .as_ref()
        .and_then(int_value)
}

fn manifest_has_live_files(manifest: &ManifestListEntry) -> bool {
    match (manifest.added_files_count, manifest.existing_files_count) {
        (Some(added), Some(existing)) => added + existing > 0,
        _ => true,
    }
}

fn file_entry(file: &DataFile, partition: &HourPartition) -> Option<(i64, Option<i64>)> {
    let bound = file
        .lower_bounds()
        .as_ref()
        .and_then(|bounds| bounds.get(&partition.source_id))
        .and_then(|value| bound_ns(value, &partition.source_type));
    let hour = file
        .partition()
        .get(&partition.name)
        .and_then(Option::as_ref)
        .and_then(int_value)
        .or_else(|| bound.map(|ns| ns.div_euclid(NANOS_PER_HOUR)))?;
    Some((hour, bound))
}

/// The earliest event timestamp, in unix nanoseconds, that `table` holds,
/// from Iceberg file statistics. `None` for an empty table or one without an
/// hour partition.
///
/// Manifests are read oldest hour first, and reading stops once the next
/// manifest's summary starts after the oldest hour already found, so usually
/// only the manifests covering that hour are opened.
pub async fn oldest_event_ns(table: &Table) -> Result<Option<i64>> {
    let Some(partition) = hour_partition(table)? else {
        return Ok(None);
    };
    let mut manifests: Vec<(Option<i64>, ManifestListEntry)> = table
        .manifests(None, None)
        .await
        .context("failed to read the manifest list")?
        .into_iter()
        .filter(|m| matches!(m.content, Content::Data) && manifest_has_live_files(m))
        .map(|m| (manifest_min_hour(&m, partition.position), m))
        .collect();
    // Manifests without a summary sort first: they must always be read.
    manifests.sort_by_key(|(hour, _)| *hour);

    let mut files: Vec<(i64, Option<i64>)> = Vec::new();
    let mut oldest_hour: Option<i64> = None;
    for (summary_hour, manifest) in manifests {
        if let (Some(summary), Some(oldest)) = (summary_hour, oldest_hour)
            && summary > oldest
        {
            break;
        }
        let one = [manifest];
        let stream = table
            .datafiles(&one, None, (None, None))
            .await
            .context("failed to read a manifest")?;
        let mut stream = std::pin::pin!(stream);
        while let Some(entry) = stream.next().await {
            let (_, entry) = entry.context("failed to read a manifest entry")?;
            if *entry.status() == Status::Deleted {
                continue;
            }
            if let Some(file) = file_entry(entry.data_file(), &partition) {
                oldest_hour = Some(oldest_hour.map_or(file.0, |h| h.min(file.0)));
                files.push(file);
            }
        }
    }
    Ok(earliest_event_ns(files))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = NANOS_PER_HOUR;

    #[test]
    fn picks_the_smallest_bound_in_the_lowest_hour() {
        let files = [
            (11, Some(11 * HOUR + 5)),
            (10, Some(10 * HOUR + 900)),
            (10, Some(10 * HOUR + 300)),
            (12, Some(12 * HOUR)),
        ];
        assert_eq!(earliest_event_ns(files), Some(10 * HOUR + 300));
    }

    #[test]
    fn falls_back_to_the_hour_floor_when_a_file_has_no_bound() {
        assert_eq!(earliest_event_ns([(10, None)]), Some(10 * HOUR));
        assert_eq!(
            earliest_event_ns([(10, Some(10 * HOUR + 300)), (10, None), (9, None)]),
            Some(9 * HOUR)
        );
        assert_eq!(
            earliest_event_ns([(10, Some(10 * HOUR + 300)), (10, None)]),
            Some(10 * HOUR),
            "an unbounded file in the oldest hour could hold anything in it"
        );
    }

    #[test]
    fn a_bound_in_a_later_hour_never_wins() {
        assert_eq!(
            earliest_event_ns([(10, None), (11, Some(11 * HOUR))]),
            Some(10 * HOUR)
        );
    }

    #[test]
    fn no_files_means_no_answer() {
        assert_eq!(earliest_event_ns(std::iter::empty()), None);
    }

    #[test]
    fn timestamp_bounds_convert_by_column_type() {
        let micros = Type::Primitive(PrimitiveType::Timestamp);
        let nanos = Type::Primitive(PrimitiveType::TimestampNs);
        let long = Type::Primitive(PrimitiveType::Long);
        assert_eq!(bound_ns(&Value::Timestamp(7), &micros), Some(7_000));
        assert_eq!(bound_ns(&Value::Timestamp(7), &nanos), Some(7));
        assert_eq!(bound_ns(&Value::LongInt(7), &long), Some(7));
        assert_eq!(bound_ns(&Value::Int(7), &micros), None);
    }
}
