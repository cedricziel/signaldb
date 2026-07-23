//! # Flamegraph Aggregation
//!
//! Merges profiles into a single hierarchical flamegraph in the Pyroscope
//! flamebearer format that Grafana renders natively.
//!
//! Samples are accumulated into a prefix tree keyed by stack frames
//! (root-first). The tree is then flattened level by level; each block is
//! encoded as `[offset_delta, total, self, name_index]`, with the offset
//! delta relative to the end of the previous block on the same level —
//! exactly the encoding `flamebearer.levels` expects.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::profile::Profile;

/// A flamegraph in Pyroscope flamebearer encoding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flamegraph {
    /// Function name table referenced by the blocks' name indices.
    pub names: Vec<String>,
    /// One entry per depth level; each level is a flat sequence of
    /// `[offset_delta, total, self, name_index]` quadruples.
    pub levels: Vec<Vec<i64>>,
    /// Total value of the root (sum of all samples).
    pub total: i64,
    /// Largest self value of any block, used for color scaling.
    pub max_self: i64,
}

/// Aggregation tree node: values accumulate on every path node, self value
/// only on the node where a stack ends.
#[derive(Default)]
struct Node {
    total: i64,
    self_value: i64,
    /// Child insertion order is preserved so sibling layout is stable.
    children: Vec<(String, Node)>,
}

impl Node {
    fn child_mut(&mut self, name: &str) -> &mut Node {
        if let Some(index) = self.children.iter().position(|(n, _)| n == name) {
            return &mut self.children[index].1;
        }
        self.children.push((name.to_string(), Node::default()));
        &mut self
            .children
            .last_mut()
            .expect("children cannot be empty after push")
            .1
    }
}

/// Aggregate profiles into one flamegraph, merging identical stacks across
/// profiles. Sample values follow the OTLP shape rules: the first value of
/// a sample, or the observation count for timestamp-only samples.
pub fn aggregate_profiles_to_flamegraph(profiles: &[Profile]) -> Flamegraph {
    let mut root = Node::default();

    for profile in profiles {
        for sample in &profile.samples {
            let value = sample
                .values
                .first()
                .copied()
                .unwrap_or(sample.timestamps_unix_nano.len() as i64);
            if value <= 0 {
                continue;
            }

            let Some(stacktrace) = profile.stacktraces.get(sample.stacktrace_index) else {
                continue;
            };

            root.total += value;
            // Stack frames are stored leaf-first; walk root-first.
            let mut node = &mut root;
            for frame in stacktrace.frames.iter().rev() {
                let name = if frame.function_name.is_empty() {
                    if frame.address != 0 {
                        format!("{:#x}", frame.address)
                    } else {
                        "<unknown>".to_string()
                    }
                } else {
                    frame.function_name.clone()
                };
                node = node.child_mut(&name);
                node.total += value;
            }
            node.self_value += value;
        }
    }

    // Empty stacks contribute directly to the root's self value.
    root.self_value = root.total
        - root
            .children
            .iter()
            .map(|(_, child)| child.total)
            .sum::<i64>();

    flatten(&root)
}

/// Flatten the aggregation tree into flamebearer levels.
fn flatten(root: &Node) -> Flamegraph {
    let mut names = Vec::new();
    let mut name_indices: HashMap<String, usize> = HashMap::new();
    let mut intern = |name: &str, names: &mut Vec<String>| -> i64 {
        if let Some(&index) = name_indices.get(name) {
            return index as i64;
        }
        let index = names.len();
        names.push(name.to_string());
        name_indices.insert(name.to_string(), index);
        index as i64
    };

    let mut levels: Vec<Vec<i64>> = Vec::new();
    let mut max_self = root.self_value;

    // Blocks to lay out at the current level: (absolute x offset, name, node).
    let root_index = intern("total", &mut names);
    levels.push(vec![0, root.total, root.self_value, root_index]);
    let mut current: Vec<(i64, &Node)> = vec![(0, root)];

    while !current.is_empty() {
        let mut next: Vec<(i64, &Node)> = Vec::new();
        let mut level: Vec<i64> = Vec::new();
        let mut previous_end: i64 = 0;

        for (offset, node) in &current {
            // Children start where the parent starts; the parent's self
            // value occupies the tail of its extent.
            let mut x = *offset;
            for (name, child) in &node.children {
                let name_index = intern(name, &mut names);
                level.extend_from_slice(&[
                    x - previous_end,
                    child.total,
                    child.self_value,
                    name_index,
                ]);
                max_self = max_self.max(child.self_value);
                next.push((x, child));
                previous_end = x + child.total;
                x += child.total;
            }
        }

        if !level.is_empty() {
            levels.push(level);
        }
        current = next;
    }

    Flamegraph {
        names,
        levels,
        total: root.total,
        max_self,
    }
}

/// A differential flamegraph in Pyroscope "double" flamebearer encoding:
/// every block carries baseline (left) and comparison (right) values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffFlamegraph {
    /// Function name table referenced by the blocks' name indices.
    pub names: Vec<String>,
    /// One entry per depth level; each level is a flat sequence of
    /// `[offset_delta_left, total_left, self_left,
    ///   offset_delta_right, total_right, self_right, name_index]`
    /// septuples, offsets delta-encoded per side.
    pub levels: Vec<Vec<i64>>,
    /// Total baseline value.
    pub left_ticks: i64,
    /// Total comparison value.
    pub right_ticks: i64,
    /// Combined total (left + right), the root block width.
    pub total: i64,
    /// Largest self value of any block on either side.
    pub max_self: i64,
}

/// Merged two-sided aggregation node.
#[derive(Default)]
struct DiffNode {
    left_total: i64,
    left_self: i64,
    right_total: i64,
    right_self: i64,
    children: Vec<(String, DiffNode)>,
}

impl DiffNode {
    fn child_mut(&mut self, name: &str) -> &mut DiffNode {
        if let Some(index) = self.children.iter().position(|(n, _)| n == name) {
            return &mut self.children[index].1;
        }
        self.children.push((name.to_string(), DiffNode::default()));
        &mut self
            .children
            .last_mut()
            .expect("children cannot be empty after push")
            .1
    }
}

fn frame_name(frame: &crate::model::profile::Frame) -> String {
    if frame.function_name.is_empty() {
        if frame.address != 0 {
            format!("{:#x}", frame.address)
        } else {
            "<unknown>".to_string()
        }
    } else {
        frame.function_name.clone()
    }
}

fn accumulate_into_diff(root: &mut DiffNode, profiles: &[Profile], right: bool) {
    for profile in profiles {
        for sample in &profile.samples {
            let value = sample
                .values
                .first()
                .copied()
                .unwrap_or(sample.timestamps_unix_nano.len() as i64);
            if value <= 0 {
                continue;
            }
            let Some(stacktrace) = profile.stacktraces.get(sample.stacktrace_index) else {
                continue;
            };

            if right {
                root.right_total += value;
            } else {
                root.left_total += value;
            }
            let mut node = &mut *root;
            for frame in stacktrace.frames.iter().rev() {
                let name = frame_name(frame);
                node = node.child_mut(&name);
                if right {
                    node.right_total += value;
                } else {
                    node.left_total += value;
                }
            }
            if right {
                node.right_self += value;
            } else {
                node.left_self += value;
            }
        }
    }
}

/// Aggregate a baseline and a comparison profile set into a differential
/// flamegraph. Stacks present on only one side get zero values on the
/// other, which renderers show as pure growth/regression.
pub fn aggregate_profiles_to_diff_flamegraph(
    baseline: &[Profile],
    comparison: &[Profile],
) -> DiffFlamegraph {
    let mut root = DiffNode::default();
    accumulate_into_diff(&mut root, baseline, false);
    accumulate_into_diff(&mut root, comparison, true);

    root.left_self = root.left_total - root.children.iter().map(|(_, c)| c.left_total).sum::<i64>();
    root.right_self = root.right_total
        - root
            .children
            .iter()
            .map(|(_, c)| c.right_total)
            .sum::<i64>();

    flatten_diff(&root)
}

fn flatten_diff(root: &DiffNode) -> DiffFlamegraph {
    let mut names = Vec::new();
    let mut name_indices: HashMap<String, usize> = HashMap::new();
    let mut intern = |name: &str, names: &mut Vec<String>| -> i64 {
        if let Some(&index) = name_indices.get(name) {
            return index as i64;
        }
        let index = names.len();
        names.push(name.to_string());
        name_indices.insert(name.to_string(), index);
        index as i64
    };

    let mut max_self = root.left_self.max(root.right_self);
    let root_index = intern("total", &mut names);
    let mut levels: Vec<Vec<i64>> = vec![vec![
        0,
        root.left_total,
        root.left_self,
        0,
        root.right_total,
        root.right_self,
        root_index,
    ]];

    // Blocks to lay out: (left absolute offset, right absolute offset, node).
    let mut current: Vec<(i64, i64, &DiffNode)> = vec![(0, 0, root)];

    while !current.is_empty() {
        let mut next: Vec<(i64, i64, &DiffNode)> = Vec::new();
        let mut level: Vec<i64> = Vec::new();
        let mut previous_left_end: i64 = 0;
        let mut previous_right_end: i64 = 0;

        for (left_offset, right_offset, node) in &current {
            let mut left_x = *left_offset;
            let mut right_x = *right_offset;
            for (name, child) in &node.children {
                let name_index = intern(name, &mut names);
                level.extend_from_slice(&[
                    left_x - previous_left_end,
                    child.left_total,
                    child.left_self,
                    right_x - previous_right_end,
                    child.right_total,
                    child.right_self,
                    name_index,
                ]);
                max_self = max_self.max(child.left_self).max(child.right_self);
                next.push((left_x, right_x, child));
                previous_left_end = left_x + child.left_total;
                previous_right_end = right_x + child.right_total;
                left_x += child.left_total;
                right_x += child.right_total;
            }
        }

        if !level.is_empty() {
            levels.push(level);
        }
        current = next;
    }

    DiffFlamegraph {
        names,
        levels,
        left_ticks: root.left_total,
        right_ticks: root.right_total,
        total: root.left_total + root.right_total,
        max_self,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::profile::{Frame, Sample, Stacktrace};

    fn frame(name: &str) -> Frame {
        Frame {
            function_name: name.to_string(),
            ..Frame::default()
        }
    }

    /// Profile with stacks (leaf-first): [work, main] x100, [idle, main] x50,
    /// and [other_root] x25.
    fn sample_profile() -> Profile {
        Profile {
            stacktraces: vec![
                Stacktrace {
                    frames: vec![frame("work"), frame("main")],
                },
                Stacktrace {
                    frames: vec![frame("idle"), frame("main")],
                },
                Stacktrace {
                    frames: vec![frame("other_root")],
                },
            ],
            samples: vec![
                Sample {
                    stacktrace_index: 0,
                    values: vec![100],
                    ..Sample::default()
                },
                Sample {
                    stacktrace_index: 1,
                    values: vec![50],
                    ..Sample::default()
                },
                Sample {
                    stacktrace_index: 2,
                    values: vec![25],
                    ..Sample::default()
                },
            ],
            ..Profile::default()
        }
    }

    #[test]
    fn aggregates_stacks_into_flamebearer_levels() {
        let flamegraph = aggregate_profiles_to_flamegraph(&[sample_profile()]);

        assert_eq!(flamegraph.total, 175);
        assert_eq!(flamegraph.names[0], "total");

        // Level 0: single root block covering everything.
        assert_eq!(flamegraph.levels[0], vec![0, 175, 0, 0]);

        // Level 1: main (150) then other_root (25), delta-encoded.
        let level1 = &flamegraph.levels[1];
        assert_eq!(level1.len(), 8);
        let main_index = flamegraph.names.iter().position(|n| n == "main").unwrap() as i64;
        let other_index = flamegraph
            .names
            .iter()
            .position(|n| n == "other_root")
            .unwrap() as i64;
        assert_eq!(&level1[0..4], &[0, 150, 0, main_index]);
        // other_root starts at x=150; previous block ended at 150 → delta 0.
        assert_eq!(&level1[4..8], &[0, 25, 25, other_index]);

        // Level 2: work (100) and idle (50) under main.
        let level2 = &flamegraph.levels[2];
        assert_eq!(level2.len(), 8);
        let work_index = flamegraph.names.iter().position(|n| n == "work").unwrap() as i64;
        let idle_index = flamegraph.names.iter().position(|n| n == "idle").unwrap() as i64;
        assert_eq!(&level2[0..4], &[0, 100, 100, work_index]);
        assert_eq!(&level2[4..8], &[0, 50, 50, idle_index]);

        assert_eq!(flamegraph.max_self, 100);
    }

    #[test]
    fn merges_identical_stacks_across_profiles() {
        let flamegraph = aggregate_profiles_to_flamegraph(&[sample_profile(), sample_profile()]);
        assert_eq!(flamegraph.total, 350);
        // Same tree shape, doubled values.
        assert_eq!(flamegraph.levels[1][1], 300); // main
        assert_eq!(flamegraph.levels[2][1], 200); // work
        // Names are interned once, not duplicated.
        assert_eq!(
            flamegraph.names.len(),
            5 // total, main, other_root, work, idle
        );
    }

    #[test]
    fn timestamp_only_samples_count_observations() {
        let mut profile = sample_profile();
        profile.samples.push(Sample {
            stacktrace_index: 0,
            values: vec![],
            timestamps_unix_nano: vec![1, 2, 3],
            ..Sample::default()
        });
        let flamegraph = aggregate_profiles_to_flamegraph(&[profile]);
        assert_eq!(flamegraph.total, 178);
    }

    #[test]
    fn empty_input_produces_empty_flamegraph() {
        let flamegraph = aggregate_profiles_to_flamegraph(&[]);
        assert_eq!(flamegraph.total, 0);
        assert_eq!(flamegraph.levels.len(), 1);
        assert_eq!(flamegraph.names, vec!["total".to_string()]);
    }

    #[test]
    fn diff_flamegraph_carries_both_sides() {
        let baseline = sample_profile(); // work 100, idle 50, other_root 25
        let mut comparison = sample_profile();
        comparison.samples[0].values = vec![200]; // work regressed to 200
        comparison.samples.remove(2); // other_root gone in comparison

        let diff = aggregate_profiles_to_diff_flamegraph(&[baseline], &[comparison]);
        assert_eq!(diff.left_ticks, 175);
        assert_eq!(diff.right_ticks, 250);
        assert_eq!(diff.total, 425);

        // Root block: [0, 175, selfL, 0, 250, selfR, 0]
        assert_eq!(diff.levels[0][1], 175);
        assert_eq!(diff.levels[0][4], 250);

        // Find the "work" block on level 2 and check both sides.
        let work_index = diff.names.iter().position(|n| n == "work").unwrap() as i64;
        let level2 = &diff.levels[2];
        let block = level2
            .chunks(7)
            .find(|c| c[6] == work_index)
            .expect("work block");
        assert_eq!(block[1], 100); // baseline total
        assert_eq!(block[4], 200); // comparison total

        // other_root exists only in baseline: right side is zero.
        let other_index = diff.names.iter().position(|n| n == "other_root").unwrap() as i64;
        let level1 = &diff.levels[1];
        let block = level1
            .chunks(7)
            .find(|c| c[6] == other_index)
            .expect("other_root block");
        assert_eq!(block[1], 25);
        assert_eq!(block[4], 0);
    }

    #[test]
    fn sibling_offsets_are_delta_encoded_across_parents() {
        // Two root children, each with one child, to verify the delta
        // between blocks that belong to different parents.
        let profile = Profile {
            stacktraces: vec![
                Stacktrace {
                    frames: vec![frame("a_leaf"), frame("a")],
                },
                Stacktrace {
                    frames: vec![frame("b_leaf"), frame("b")],
                },
            ],
            samples: vec![
                Sample {
                    stacktrace_index: 0,
                    values: vec![60],
                    ..Sample::default()
                },
                Sample {
                    stacktrace_index: 1,
                    values: vec![40],
                    ..Sample::default()
                },
            ],
            ..Profile::default()
        };

        let flamegraph = aggregate_profiles_to_flamegraph(&[profile]);
        // Level 2: a_leaf under a at x=0, b_leaf under b at x=60.
        let level2 = &flamegraph.levels[2];
        assert_eq!(&level2[0..3], &[0, 60, 60]);
        // Previous block ended at 60, b_leaf starts at 60 → delta 0.
        assert_eq!(&level2[4..7], &[0, 40, 40]);
    }
}
