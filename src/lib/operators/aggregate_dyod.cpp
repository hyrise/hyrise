#include "aggregate_dyod.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/operator_state.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "type_comparison.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {
using namespace hyrise;

// The number of bytes stored for the prefix of string columns.
constexpr auto STRING_PREFIX_SIZE = size_t{7};

// The layout info used to decode a GROUP BY column from the normalized key later on.
struct ColumnKeyLayout {
  size_t byte_offset{0};
  // Only needed when this is a string column, otherwise unused.
  size_t string_slot_index{0};
};

struct NormalizedKeyInfo {
  size_t key_size{0};
  bool has_string_column{false};
  size_t string_column_count{0};
  std::vector<ColumnKeyLayout> column_layouts;
};

// Computes the byte layout of the normalized GROUP BY key for the given table and column ids.
NormalizedKeyInfo compute_normalized_key_info(const std::shared_ptr<const Table>& table,
                                              const std::vector<ColumnID>& column_ids) {
  auto key_info = NormalizedKeyInfo{};
  key_info.column_layouts.resize(column_ids.size());
  for (auto column_index = size_t{0}; column_index < column_ids.size(); ++column_index) {
    const auto data_type = table->column_data_type(column_ids[column_index]);
    auto& layout = key_info.column_layouts[column_index];
    layout.byte_offset = key_info.key_size;

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        layout.string_slot_index = key_info.string_column_count;
        // Key size consists of: null byte + length byte + prefix.
        key_info.key_size += 2 + STRING_PREFIX_SIZE;
        key_info.has_string_column = true;
        ++key_info.string_column_count;
      } else {
        // Key size consists of: null byte + prefix.
        key_info.key_size += sizeof(ColumnDataType) + 1;
      }
    });
  }
  return key_info;
}

// Result of hash-partitioning the input rows by their normalized GROUP BY key.
struct GroupByPartitions {
  NormalizedKeyInfo key_info;
  std::vector<uint8_t> materialized_key_bytes;
  std::vector<pmr_string> row_strings;
  std::vector<size_t> row_hashes;

  size_t partition_count{0};
  std::vector<size_t> partition_start;
  std::vector<size_t> partition_size;
  std::vector<size_t> rows;

  std::vector<size_t> group_count;
  std::vector<std::vector<size_t>> row_to_group;
  std::vector<std::vector<size_t>> group_representative_row;
  std::vector<std::shared_ptr<AbstractTask>> hash_table_ready;

  // Only used while splitting a skewed partition into sub-jobs, cleared once merged.
  std::vector<std::vector<std::vector<size_t>>> sub_row_to_group;
  std::vector<std::vector<std::vector<size_t>>> sub_representative_row;
  std::vector<std::vector<size_t>> sub_slice_start;
};

// Struct for a single materialized aggregate column.
struct MaterializedAggregateColumn {
  std::shared_ptr<void> values;
  std::shared_ptr<std::vector<uint8_t>> nulls;
  std::shared_ptr<AbstractTask> materialization_task;
};

// Cache of materialized aggregate columns, so we don't have to materialize the same column multiple times.
struct MaterializedColumnCache {
  std::unordered_map<ColumnID, MaterializedAggregateColumn> entries;
};

// Materializes the GROUP BY columns in parallel, one job per (chunk, column) pair.
std::vector<uint8_t> materialize_groupby_keys(const std::shared_ptr<const Table>& table,
                                              const std::vector<ColumnID>& column_ids,
                                              const std::vector<ColumnKeyLayout>& column_layouts,
                                              const size_t normalized_key_size, const size_t string_column_count,
                                              const std::vector<size_t>& chunk_row_offset,
                                              std::vector<pmr_string>& row_strings) {
  const auto row_count = table->row_count();
  const auto chunk_count = table->chunk_count();

  auto materialized_data = std::vector<uint8_t>(row_count * normalized_key_size);

  auto materialize_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  materialize_jobs.reserve(static_cast<size_t>(chunk_count) * column_ids.size());

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    for (auto column_index = size_t{0}; column_index < column_ids.size(); ++column_index) {
      materialize_jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id, column_index]() {
        const auto chunk = table->get_chunk(chunk_id);
        const auto global_row_offset = chunk_row_offset[chunk_id];

        const auto column_id = column_ids[column_index];
        const auto& layout = column_layouts[column_index];
        const auto& segment = chunk->get_segment(column_id);
        const auto data_type = table->column_data_type(column_id);

        resolve_data_type(data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
            segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
              const auto global_row_idx = global_row_offset + position.chunk_offset();

              // Get a direct pointer to where this column starts for this specific row.
              auto* write_ptr =
                  materialized_data.data() + (global_row_idx * normalized_key_size) + layout.byte_offset;

              if (position.is_null()) {
                // Write NULL marker.
                *write_ptr = 1;
              } else {
                // Write NOT NULL marker.
                *write_ptr = 0;

                const auto& str = position.value();
                // The length byte makes the key exact for strings that fit the prefix.
                // For longer strings which exceed the prefix we need to compare the full string.
                write_ptr[1] = static_cast<uint8_t>(std::min(str.size(), size_t{255}));
                // Short strings get zero-padded.
                std::memset(write_ptr + 2, 0, STRING_PREFIX_SIZE);
                std::memcpy(write_ptr + 2, str.data(), std::min(str.size(), STRING_PREFIX_SIZE));

                // Only store this where we need it.
                if (str.size() > STRING_PREFIX_SIZE) {
                  row_strings[(global_row_idx * string_column_count) + layout.string_slot_index] = str;
                }
              }
            });
          } else {
            constexpr size_t TYPE_SIZE = sizeof(ColumnDataType);

            segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
              const auto global_row_idx = global_row_offset + position.chunk_offset();

              // Get a direct pointer to where this column starts for this specific row.
              auto* write_ptr =
                  materialized_data.data() + (global_row_idx * normalized_key_size) + layout.byte_offset;

              if (position.is_null()) {
                // Write NULL marker.
                *write_ptr = 1;
              } else {
                // Write NOT NULL marker.
                *write_ptr = 0;

                // Copy the actual value starting 1 byte after the null marker.
                const auto& value = position.value();
                std::memcpy(write_ptr + 1, &value, TYPE_SIZE);
              }
            });
          }
        });
      }));
    }
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(materialize_jobs);

  return materialized_data;
}

// Computes a hash per row.
void compute_row_hashes(const std::vector<uint8_t>& materialized_data, const size_t normalized_key_size,
                        std::vector<size_t>& row_hashes) {
  const auto row_count = row_hashes.size();
  const auto num_cpus = std::max<size_t>(1, Hyrise::get().topology.num_cpus());
  const auto hash_job_count = std::min<size_t>(num_cpus, std::max<size_t>(1, row_count));

  auto hash_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  hash_jobs.reserve(hash_job_count);
  for (auto job_id = size_t{0}; job_id < hash_job_count; ++job_id) {
    const auto row_begin = row_count * job_id / hash_job_count;
    const auto row_end = row_count * (job_id + 1) / hash_job_count;
    hash_jobs.emplace_back(std::make_shared<JobTask>([&, row_begin, row_end]() {
      for (auto row_index = row_begin; row_index < row_end; ++row_index) {
        const auto* key_begin = materialized_data.data() + (row_index * normalized_key_size);
        auto seed = size_t{0};
        boost::hash_range(seed, key_begin, key_begin + normalized_key_size);
        row_hashes[row_index] = seed;
      }
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(hash_jobs);
}

// Compute global row offset per chunk, so per-row work can be parallelized across chunks.
std::vector<size_t> compute_chunk_row_offsets(const std::shared_ptr<const Table>& table) {
  const auto chunk_count = table->chunk_count();
  auto chunk_row_offset = std::vector<size_t>(chunk_count, 0);
  for (auto chunk_id = ChunkID{0}; chunk_id + 1 < chunk_count; ++chunk_id) {
    chunk_row_offset[chunk_id + 1] = chunk_row_offset[chunk_id] + table->get_chunk(chunk_id)->size();
  }
  return chunk_row_offset;
}

// Number of hash partitions to split the input rows into.
// We use the next power of two at or below ~4x the CPU count.
size_t choose_partition_count() {
  auto partition_count = size_t{1};
  const auto min_number_partitions = size_t{4} * std::max<size_t>(1, Hyrise::get().topology.num_cpus());
  while (partition_count * 2 <= min_number_partitions) {
    partition_count *= 2;
  }
  return partition_count;
}

/**
 * Counts rows per (chunk, partition), computes offsets from those counts, then scatters every row's global index
 * into `partitions.rows`, grouped by partition.
 */
void scatter_rows_into_partitions(GroupByPartitions& partitions, const std::shared_ptr<const Table>& table,
                                  const std::vector<size_t>& chunk_row_offset) {
  const auto num_rows = table->row_count();
  const auto num_chunks = table->chunk_count();
  const auto partition_count = partitions.partition_count;
  const auto partition_mask = partition_count - 1;
  const auto& row_hashes = partitions.row_hashes;

  auto partition_of_row = [&](const size_t global_row_idx) {
    return row_hashes[global_row_idx] & partition_mask;
  };

  auto chunk_partition_counts = std::vector<std::vector<size_t>>(num_chunks, std::vector<size_t>(partition_count, 0));
  {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(num_chunks);
    for (ChunkID chunk_id{0}; chunk_id < num_chunks; ++chunk_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
        const auto chunk_size = table->get_chunk(chunk_id)->size();
        const auto global_row_offset = chunk_row_offset[chunk_id];
        auto& counts = chunk_partition_counts[chunk_id];
        for (auto local_offset = size_t{0}; local_offset < chunk_size; ++local_offset) {
          ++counts[partition_of_row(global_row_offset + local_offset)];
        }
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  // Compute offsets for each partition in each chunk, so we can write the rows into a single array.
  auto chunk_partition_offset = std::vector<std::vector<size_t>>(num_chunks, std::vector<size_t>(partition_count, 0));
  partitions.partition_start.assign(partition_count, 0);
  partitions.partition_size.assign(partition_count, 0);
  auto running_chunk = size_t{0};
  for (auto partition_index = size_t{0}; partition_index < partition_count; ++partition_index) {
    partitions.partition_start[partition_index] = running_chunk;
    for (auto chunk_id = size_t{0}; chunk_id < num_chunks; ++chunk_id) {
      partitions.partition_size[partition_index] += chunk_partition_counts[chunk_id][partition_index];
      chunk_partition_offset[chunk_id][partition_index] = running_chunk;
      running_chunk += chunk_partition_counts[chunk_id][partition_index];
    }
  }

  partitions.rows.assign(num_rows, 0);
  {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(num_chunks);
    for (ChunkID chunk_id{0}; chunk_id < num_chunks; ++chunk_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
        const auto chunk_size = table->get_chunk(chunk_id)->size();
        const auto global_row_offset = chunk_row_offset[chunk_id];
        // Local copy.
        auto cursor = chunk_partition_offset[chunk_id];
        for (auto local_offset = size_t{0}; local_offset < chunk_size; ++local_offset) {
          const auto global_row_idx = global_row_offset + local_offset;
          partitions.rows[cursor[partition_of_row(global_row_idx)]++] = global_row_idx;
        }
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }
}

// Build one local hash table (normalized key -> local group id) for a single, non-skewed partition.
template <typename KeyHash, typename KeyEqual>
std::shared_ptr<AbstractTask> schedule_partition_hash_table_job(const size_t partition_index,
                                                                GroupByPartitions& partitions, const KeyHash& key_hash,
                                                                const KeyEqual& key_equal) {
  return std::make_shared<JobTask>([&partitions, partition_index, key_hash, key_equal]() {
    const auto begin = partitions.partition_start[partition_index];
    const auto size = partitions.partition_size[partition_index];

    auto local_hash_to_index = boost::unordered_flat_map<size_t, size_t, KeyHash, KeyEqual>(0, key_hash, key_equal);
    local_hash_to_index.reserve(size / 4);

    auto& row_to_group_p = partitions.row_to_group[partition_index];
    auto& representative_p = partitions.group_representative_row[partition_index];
    row_to_group_p.resize(size);

    auto next_local_group = size_t{0};
    for (auto row_index = size_t{0}; row_index < size; ++row_index) {
      const auto global_row_idx = partitions.rows[begin + row_index];
      auto [it, inserted] = local_hash_to_index.try_emplace(global_row_idx, next_local_group);
      row_to_group_p[row_index] = it->second;
      if (inserted) {
        representative_p.push_back(global_row_idx);
        ++next_local_group;
      }
    }
    partitions.group_count[partition_index] = next_local_group;
  });
}
/**
 * Build a skewed partition's hash table by splitting it into `sub_count` sub-jobs,
 * each building an independent local hash table, followed by a merge job that combines them.
 */
template <typename KeyHash, typename KeyEqual>
std::shared_ptr<AbstractTask> schedule_skewed_partition_hash_table_job(
    const size_t partition_index, const size_t sub_count, GroupByPartitions& partitions, const KeyHash& key_hash,
    const KeyEqual& key_equal, std::vector<std::shared_ptr<AbstractTask>>& all_jobs) {
  const auto begin = partitions.partition_start[partition_index];
  const auto partition_row_count = partitions.partition_size[partition_index];

  auto& slice_start = partitions.sub_slice_start[partition_index];
  slice_start.resize(sub_count + 1);
  for (auto sub_partition_index = size_t{0}; sub_partition_index <= sub_count; ++sub_partition_index) {
    slice_start[sub_partition_index] = begin + (partition_row_count * sub_partition_index / sub_count);
  }

  partitions.sub_row_to_group[partition_index].assign(sub_count, {});
  partitions.sub_representative_row[partition_index].assign(sub_count, {});

  auto sub_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  sub_jobs.reserve(sub_count);
  for (auto sub_partition_index = size_t{0}; sub_partition_index < sub_count; ++sub_partition_index) {
    auto sub_job =
        std::make_shared<JobTask>([&partitions, partition_index, sub_partition_index, key_hash, key_equal]() {
          const auto sub_begin = partitions.sub_slice_start[partition_index][sub_partition_index];
          const auto sub_size = partitions.sub_slice_start[partition_index][sub_partition_index + 1] - sub_begin;

          auto local_hash_to_index =
              boost::unordered_flat_map<size_t, size_t, KeyHash, KeyEqual>(0, key_hash, key_equal);
          local_hash_to_index.reserve(sub_size / 4);

          auto& row_to_group_s = partitions.sub_row_to_group[partition_index][sub_partition_index];
          auto& representative_s = partitions.sub_representative_row[partition_index][sub_partition_index];
          row_to_group_s.resize(sub_size);

          auto next_local_group = size_t{0};
          for (auto row_index = size_t{0}; row_index < sub_size; ++row_index) {
            const auto global_row_idx = partitions.rows[sub_begin + row_index];
            auto [it, inserted] = local_hash_to_index.try_emplace(global_row_idx, next_local_group);
            row_to_group_s[row_index] = it->second;
            if (inserted) {
              representative_s.push_back(global_row_idx);
              ++next_local_group;
            }
          }
        });
    sub_jobs.push_back(sub_job);
    all_jobs.push_back(sub_job);
  }

  auto merge_job = std::make_shared<JobTask>([&partitions, partition_index, key_hash, key_equal]() {
    const auto merge_begin = partitions.partition_start[partition_index];
    const auto merge_size = partitions.partition_size[partition_index];
    const auto sub_count_p = partitions.sub_representative_row[partition_index].size();

    auto combined_hash_to_index = boost::unordered_flat_map<size_t, size_t, KeyHash, KeyEqual>(0, key_hash, key_equal);

    auto& row_to_group_p = partitions.row_to_group[partition_index];
    auto& representative_p = partitions.group_representative_row[partition_index];
    row_to_group_p.resize(merge_size);

    auto next_merged_group = size_t{0};
    for (auto sub_partition_index = size_t{0}; sub_partition_index < sub_count_p; ++sub_partition_index) {
      const auto& representative_s = partitions.sub_representative_row[partition_index][sub_partition_index];
      const auto sub_local_count = representative_s.size();

      auto remap = std::vector<size_t>(sub_local_count);
      for (auto local_group = size_t{0}; local_group < sub_local_count; ++local_group) {
        const auto representative_row = representative_s[local_group];
        auto [it, inserted] = combined_hash_to_index.try_emplace(representative_row, next_merged_group);
        remap[local_group] = it->second;
        if (inserted) {
          representative_p.push_back(representative_row);
          ++next_merged_group;
        }
      }

      const auto sub_local_begin = partitions.sub_slice_start[partition_index][sub_partition_index] - merge_begin;
      const auto& row_to_group_s = partitions.sub_row_to_group[partition_index][sub_partition_index];
      const auto sub_size = row_to_group_s.size();
      for (auto local_row_index = size_t{0}; local_row_index < sub_size; ++local_row_index) {
        row_to_group_p[sub_local_begin + local_row_index] = remap[row_to_group_s[local_row_index]];
      }
    }

    partitions.group_count[partition_index] = next_merged_group;

    partitions.sub_row_to_group[partition_index].clear();
    partitions.sub_row_to_group[partition_index].shrink_to_fit();
    partitions.sub_representative_row[partition_index].clear();
    partitions.sub_representative_row[partition_index].shrink_to_fit();
  });

  for (auto& sub_job : sub_jobs) {
    sub_job->set_as_predecessor_of(merge_job);
  }
  all_jobs.push_back(merge_job);
  return merge_job;
}

// Builds every partition's local hash table, splitting a partition into sub-jobs first if it is skewed.
template <typename KeyHash, typename KeyEqual>
void build_local_hash_tables(GroupByPartitions& partitions, const KeyHash& key_hash, const KeyEqual& key_equal,
                             std::vector<std::shared_ptr<AbstractTask>>& all_jobs) {
  const auto partition_count = partitions.partition_count;
  const auto num_rows = partitions.rows.size();

  partitions.group_count.assign(partition_count, 0);
  partitions.row_to_group.assign(partition_count, {});
  partitions.group_representative_row.assign(partition_count, {});
  partitions.hash_table_ready.assign(partition_count, nullptr);
  partitions.sub_row_to_group.assign(partition_count, {});
  partitions.sub_representative_row.assign(partition_count, {});
  partitions.sub_slice_start.assign(partition_count, {});

  const auto average_partition_size = std::max<size_t>(1, num_rows / partition_count);
  constexpr auto SKEW_THRESHOLD = size_t{4};
  const auto num_cpus = std::max<size_t>(1, Hyrise::get().topology.num_cpus());

  for (auto partition_index = size_t{0}; partition_index < partition_count; ++partition_index) {
    const auto partition_row_count = partitions.partition_size[partition_index];
    if (partition_row_count == 0) {
      continue;
    }

    auto sub_count = size_t{1};
    if (partition_row_count > SKEW_THRESHOLD * average_partition_size) {
      sub_count = std::clamp<size_t>((partition_row_count + average_partition_size - 1) / average_partition_size,
                                     size_t{2}, num_cpus);
    }

    if (sub_count == 1) {
      auto hash_job = schedule_partition_hash_table_job(partition_index, partitions, key_hash, key_equal);
      partitions.hash_table_ready[partition_index] = hash_job;
      all_jobs.push_back(std::move(hash_job));
      continue;
    }

    partitions.hash_table_ready[partition_index] =
        schedule_skewed_partition_hash_table_job(partition_index, sub_count, partitions, key_hash, key_equal, all_jobs);
  }
}
/**
 * Hash-partitions the input rows by their normalized GROUP BY key,
 * by building one local hash table (group id lookup) per partition.
 */
void partition_by_groupby_keys(const std::shared_ptr<const Table>& input_table,
                               const std::vector<ColumnID>& groupby_column_ids,
                               const std::vector<size_t>& chunk_row_offset, GroupByPartitions& partitions,
                               std::vector<std::shared_ptr<AbstractTask>>& all_jobs) {
  const auto num_rows = input_table->row_count();

  partitions.key_info = compute_normalized_key_info(input_table, groupby_column_ids);
  const auto normalized_key_size = partitions.key_info.key_size;

  partitions.row_strings.resize(
      partitions.key_info.has_string_column ? num_rows * partitions.key_info.string_column_count : 0);
  partitions.row_hashes.resize(num_rows);

  partitions.materialized_key_bytes =
      materialize_groupby_keys(input_table, groupby_column_ids, partitions.key_info.column_layouts, normalized_key_size,
                               partitions.key_info.string_column_count, chunk_row_offset, partitions.row_strings);
  compute_row_hashes(partitions.materialized_key_bytes, normalized_key_size, partitions.row_hashes);

  partitions.partition_count = choose_partition_count();
  scatter_rows_into_partitions(partitions, input_table, chunk_row_offset);

  auto key_hash = [&partitions](const size_t row_index) -> size_t {
    return partitions.row_hashes[row_index];
  };
  auto key_equal = [&partitions, normalized_key_size](const size_t row_a, const size_t row_b) -> bool {
    const auto* data = partitions.materialized_key_bytes.data();
    if (std::memcmp(data + (row_a * normalized_key_size), data + (row_b * normalized_key_size), normalized_key_size) !=
        0) {
      return false;
    }
    if (!partitions.key_info.has_string_column) {
      return true;
    }

    const auto string_column_count = partitions.key_info.string_column_count;
    const auto* row_a_strings = partitions.row_strings.data() + (row_a * string_column_count);
    const auto* row_b_strings = partitions.row_strings.data() + (row_b * string_column_count);
    return std::equal(row_a_strings, row_a_strings + string_column_count, row_b_strings);
  };

  build_local_hash_tables(partitions, key_hash, key_equal, all_jobs);
}

// Writes this aggregate's entry in `output_column_definitions` and returns {output_column_id, needs_null}.
template <typename ColumnDataType, WindowFunction aggregate_function>
std::pair<size_t, bool> prepare_aggregate_output_column(const std::shared_ptr<WindowFunctionExpression>& aggregate,
                                                        const size_t aggregate_index,
                                                        const std::shared_ptr<const Table>& input_table,
                                                        const ColumnID input_column_id,
                                                        const size_t groupby_column_count,
                                                        TableColumnDefinitions& output_column_definitions) {
  const auto result_type = WindowFunctionTraits<ColumnDataType, aggregate_function>::RESULT_TYPE;

  auto needs_null = true;
  if constexpr (aggregate_function == WindowFunction::Count || aggregate_function == WindowFunction::CountDistinct) {
    needs_null = false;
  } else if constexpr (aggregate_function == WindowFunction::Any) {
    // Inherit from input column.
    needs_null = input_table->column_is_nullable(input_column_id);
  }

  const auto output_column_id = groupby_column_count + aggregate_index;
  const auto column_name = (aggregate_function != WindowFunction::Any) ? aggregate->as_column_name()
                                                                       : input_table->column_name(input_column_id);

  output_column_definitions[output_column_id] = TableColumnDefinition{column_name, result_type, needs_null};

  return {output_column_id, needs_null};
}

// Converts finished AggregateResults into the output ValueSegment.
template <typename ColumnDataType, WindowFunction aggregate_function>
void fill_output_segment(AggregateResults<ColumnDataType, aggregate_function>& results, const size_t result_count,
                         const size_t output_column_id, const bool needs_null, Segments& target) {
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;

  auto agg_values = pmr_vector<AggregateType>{};
  auto agg_nulls = pmr_vector<bool>{};
  agg_values.reserve(result_count);
  auto has_null = false;

  for (auto group_index = size_t{0}; group_index < result_count; ++group_index) {
    const auto& result = results[group_index];

    if constexpr (aggregate_function == WindowFunction::Count) {
      agg_values.emplace_back(static_cast<AggregateType>(result.aggregate_count));
      agg_nulls.emplace_back(false);
    } else if constexpr (aggregate_function == WindowFunction::CountDistinct) {
      agg_values.emplace_back(static_cast<AggregateType>(result.accumulator.size()));
      agg_nulls.emplace_back(false);
    } else if constexpr (aggregate_function == WindowFunction::Avg) {
      if constexpr (std::is_arithmetic_v<AggregateType>) {
        if (result.aggregate_count > 0) {
          agg_values.emplace_back(result.accumulator / static_cast<AggregateType>(result.aggregate_count));
          agg_nulls.emplace_back(false);
        } else {
          agg_values.emplace_back();
          agg_nulls.emplace_back(true);
          has_null = true;
        }
      } else {
        Fail("AVG is not defined for non-arithmetic types.");
      }
    } else if constexpr (aggregate_function == WindowFunction::StandardDeviationSample) {
      if constexpr (std::is_arithmetic_v<AggregateType>) {
        if (result.aggregate_count > 1) {
          agg_values.emplace_back(static_cast<AggregateType>(result.accumulator[3]));
          agg_nulls.emplace_back(false);
        } else {
          agg_values.emplace_back();
          agg_nulls.emplace_back(true);
          has_null = true;
        }
      } else {
        Fail("STDDEV_SAMP requires a valid arithmetic AggregateType.");
      }
    } else {
      if constexpr (std::is_constructible_v<AggregateType, decltype(result.accumulator)>) {
        if (result.aggregate_count > 0) {
          agg_values.emplace_back(result.accumulator);
          agg_nulls.emplace_back(false);
        } else {
          agg_values.emplace_back();
          agg_nulls.emplace_back(true);
          has_null = true;
        }
      } else {
        Fail("Aggregate type is not constructible.");
      }
    }
  }

  if (has_null || needs_null) {
    target[output_column_id] =
        std::make_shared<ValueSegment<AggregateType>>(std::move(agg_values), std::move(agg_nulls));
  } else {
    target[output_column_id] = std::make_shared<ValueSegment<AggregateType>>(std::move(agg_values));
  }
}

/**
 * Accumulates one non-NULL value into an aggregate result. Shared between the trivial (no GROUP BY) and partitioned
 * accumulation loops, which otherwise only differ in how they iterate over rows.
 */
template <typename ColumnDataType, WindowFunction aggregate_function>
void accumulate_into_result(const ColumnDataType& value, AggregateResult<ColumnDataType, aggregate_function>& result) {
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;

  if constexpr (aggregate_function == WindowFunction::CountDistinct) {
    result.accumulator.emplace(value);
  } else if constexpr (aggregate_function == WindowFunction::Any) {
    // ANY just stores the first value.
    if (result.aggregate_count == 0) {
      result.accumulator = value;
    }
  } else {
    const auto aggregator =
        WindowFunctionBuilder<ColumnDataType, AggregateType, aggregate_function>().get_aggregate_function();
    aggregator(value, result.aggregate_count, result.accumulator);
  }
  ++result.aggregate_count;
}

/**
 * Per-chunk-worker state for the no-GROUP-BY path,
 * merged into one result across all workers at the end.
 */
template <typename ColumnDataType, WindowFunction aggregate_function>
class TrivialGroupAggregateState : public Noncopyable {
 public:
  void ensure_initialized(const size_t group_count) {
    if (results.empty()) {
      results.resize(group_count);
    }
  }

  void merge(TrivialGroupAggregateState& other) {
    const auto count = results.size();
    for (auto group_index = size_t{0}; group_index < count; ++group_index) {
      auto& mine = results[group_index];
      auto& theirs = other.results[group_index];

      if constexpr (aggregate_function == WindowFunction::CountDistinct) {
        for (const auto& value : theirs.accumulator) {
          mine.accumulator.insert(value);
        }
        mine.aggregate_count += theirs.aggregate_count;
      } else if constexpr (aggregate_function == WindowFunction::Any) {
        // For ANY, it doesn't matter which valid value we keep.
        if (mine.aggregate_count == 0 && theirs.aggregate_count > 0) {
          mine = theirs;
        }
      } else if constexpr (aggregate_function == WindowFunction::Min) {
        if (theirs.aggregate_count == 0) {
          // Nothing to merge.
        } else if (mine.aggregate_count == 0 || value_smaller(theirs.accumulator, mine.accumulator)) {
          mine.accumulator = theirs.accumulator;
        }
        mine.aggregate_count += theirs.aggregate_count;
      } else if constexpr (aggregate_function == WindowFunction::Max) {
        if (theirs.aggregate_count == 0) {
          // Nothing to merge.
        } else if (mine.aggregate_count == 0 || value_greater(theirs.accumulator, mine.accumulator)) {
          mine.accumulator = theirs.accumulator;
        }
        mine.aggregate_count += theirs.aggregate_count;
      } else if constexpr (aggregate_function == WindowFunction::StandardDeviationSample) {
        // Working merge for state of Welford accumulator used in abstract_aggregate_operator.hpp.
        const auto count_mine = mine.accumulator[0];
        const auto count_theirs = theirs.accumulator[0];
        const auto combined_count = count_mine + count_theirs;
        if (count_theirs > 0) {
          if (count_mine == 0) {
            mine.accumulator = theirs.accumulator;
          } else {
            const auto delta = theirs.accumulator[1] - mine.accumulator[1];
            const auto mean = mine.accumulator[1] + (delta * count_theirs / combined_count);
            const auto combined_m2 = mine.accumulator[2] + theirs.accumulator[2] +
                                     (delta * delta * count_mine * count_theirs / combined_count);
            mine.accumulator[0] = combined_count;
            mine.accumulator[1] = mean;
            mine.accumulator[2] = combined_m2;
            mine.accumulator[3] = (combined_count > 1) ? std::sqrt(combined_m2 / (combined_count - 1)) : 0.0;
          }
        }
        mine.aggregate_count += theirs.aggregate_count;
      } else {
        // For count, sum, avg we simply add, for avg the final division is done at the end (fill_output_segment).
        mine.accumulator += theirs.accumulator;
        mine.aggregate_count += theirs.aggregate_count;
      }
    }
  }

  AggregateResults<ColumnDataType, aggregate_function> results;
};

/**
 * No-GROUP-BY path: aggregates the whole column into the single implicit group,
 * one job per chunk merged via OperatorSharedState.
 */
template <typename ColumnDataType, WindowFunction aggregate_function>
void accumulate_trivial_group(const std::shared_ptr<const Table>& input_table, const ColumnID column_id,
                              const size_t group_count, const size_t output_column_id, const bool needs_null,
                              Segments& output_segments) {
  const auto num_chunks = input_table->chunk_count();
  auto aggregate_results = AggregateResults<ColumnDataType, aggregate_function>{};

  if (num_chunks == 0) {
    // Just have an empty aggregate result for the single group to avoid overhead.
    aggregate_results.resize(group_count);
  } else {
    using WorkerState = TrivialGroupAggregateState<ColumnDataType, aggregate_function>;
    auto operator_state = OperatorSharedState<WorkerState>{};

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(num_chunks);

    for (ChunkID chunk_id{0}; chunk_id < num_chunks; ++chunk_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
        auto& worker_state = operator_state.current_worker_state();
        worker_state.ensure_initialized(group_count);
        auto& results = worker_state.results;

        const auto chunk = input_table->get_chunk(chunk_id);

        if (column_id == INVALID_COLUMN_ID) {
          const auto chunk_size = chunk->size();
          results[0].aggregate_count += chunk_size;
          return;
        }

        const auto& segment = chunk->get_segment(column_id);

        if constexpr (aggregate_function == WindowFunction::Min || aggregate_function == WindowFunction::Max) {
          // We can abuse dict value_of_value_id to get the min/max value without iterating over all rows.
          if (const auto* dict_segment = dynamic_cast<const BaseDictionarySegment*>(segment.get())) {
            const auto unique_count = dict_segment->unique_values_count();
            if (unique_count > 0) {
              const auto value_id = (aggregate_function == WindowFunction::Min)
                                        ? ValueID{0}
                                        : ValueID{static_cast<ValueID::base_type>(unique_count - 1)};
              const auto extreme_value = boost::get<ColumnDataType>(dict_segment->value_of_value_id(value_id));
              accumulate_into_result<ColumnDataType, aggregate_function>(extreme_value, results[0]);
            }
            return;
          }
        }

        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          // Skip NULL values.
          if (position.is_null()) {
            return;
          }
          accumulate_into_result<ColumnDataType, aggregate_function>(position.value(), results[0]);
        });
      }));
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
    aggregate_results = std::move(operator_state.merge_worker_states().results);
  }

  fill_output_segment<ColumnDataType, aggregate_function>(aggregate_results, group_count, output_column_id, needs_null,
                                                          output_segments);
}

/**
 * Materializes an aggregate argument column into a flat values/nulls array, or reuses a cached materialization from
 * an earlier aggregate over the same column.
 */
template <typename ColumnDataType>
void materialize_aggregate_argument_column(const std::shared_ptr<const Table>& input_table, const ColumnID column_id,
                                           const std::vector<size_t>& chunk_row_offset,
                                           MaterializedColumnCache& column_cache,
                                           std::vector<std::shared_ptr<AbstractTask>>& all_jobs) {
  if (column_cache.entries.contains(column_id)) {
    return;
  }

  const auto num_rows = input_table->row_count();
  const auto num_chunks = input_table->chunk_count();

  auto values_ptr = std::make_shared<std::vector<ColumnDataType>>(num_rows);
  auto nulls_ptr = std::make_shared<std::vector<uint8_t>>(num_rows);
  auto materialization_task = std::make_shared<JobTask>([]() {});

  for (ChunkID chunk_id{0}; chunk_id < num_chunks; ++chunk_id) {
    auto materialize_job = std::make_shared<JobTask>([&, chunk_id, column_id, values_ptr, nulls_ptr]() {
      const auto chunk = input_table->get_chunk(chunk_id);
      const auto global_row_offset = chunk_row_offset[chunk_id];
      const auto& segment = chunk->get_segment(column_id);
      auto* const values = values_ptr->data();
      auto* const nulls = nulls_ptr->data();

      // Do some optimized copies for value segments, otherwise iterate over the segment.
      if (const auto* value_segment = dynamic_cast<const ValueSegment<ColumnDataType>*>(segment.get())) {
        const auto& src_values = value_segment->values();
        std::copy(src_values.cbegin(), src_values.cend(), values + global_row_offset);

        if (value_segment->is_nullable()) {
          const auto& src_nulls = value_segment->null_values();
          const auto chunk_size = src_values.size();
          for (auto offset = size_t{0}; offset < chunk_size; ++offset) {
            nulls[global_row_offset + offset] = src_nulls[offset] ? 1 : 0;
          }
        } else {
          std::fill(nulls + global_row_offset, nulls + global_row_offset + src_values.size(), uint8_t{0});
        }
        return;
      }

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        const auto global_row_idx = global_row_offset + position.chunk_offset();
        if (position.is_null()) {
          nulls[global_row_idx] = 1;
        } else {
          nulls[global_row_idx] = 0;
          values[global_row_idx] = position.value();
        }
      });
    });
    materialize_job->set_as_predecessor_of(materialization_task);
    all_jobs.push_back(materialize_job);
  }
  all_jobs.push_back(materialization_task);

  column_cache.entries.emplace(column_id, MaterializedAggregateColumn{std::static_pointer_cast<void>(values_ptr),
                                                                      nulls_ptr, materialization_task});
}

// GROUP-BY path: accumulates one aggregate independently within each partition.
template <typename ColumnDataType, WindowFunction aggregate_function>
void accumulate_partitioned_groups(const std::shared_ptr<const Table>& input_table, const ColumnID column_id,
                                   GroupByPartitions& partitions, MaterializedColumnCache& column_cache,
                                   const size_t output_column_id, const bool needs_null,
                                   const std::vector<size_t>& chunk_row_offset,
                                   std::vector<Segments>& partition_segments,
                                   std::vector<std::shared_ptr<AbstractTask>>& all_jobs) {
  auto values_ptr = std::shared_ptr<std::vector<ColumnDataType>>{};
  auto nulls_ptr = std::shared_ptr<std::vector<uint8_t>>{};
  auto materialization_task = std::shared_ptr<AbstractTask>{};

  if (column_id != INVALID_COLUMN_ID) {
    materialize_aggregate_argument_column<ColumnDataType>(input_table, column_id, chunk_row_offset, column_cache,
                                                          all_jobs);
    const auto& materialized = column_cache.entries.at(column_id);
    values_ptr = std::static_pointer_cast<std::vector<ColumnDataType>>(materialized.values);
    nulls_ptr = materialized.nulls;
    materialization_task = materialized.materialization_task;
  }

  const auto partition_count = partitions.partition_count;
  for (auto partition_index = size_t{0}; partition_index < partition_count; ++partition_index) {
    if (partitions.partition_size[partition_index] == 0) {
      continue;
    }
    auto accumulate_job = std::make_shared<JobTask>(
        [&, partition_index, column_id, values_ptr, nulls_ptr, output_column_id, needs_null]() {
          const auto begin = partitions.partition_start[partition_index];
          const auto size = partitions.partition_size[partition_index];
          const auto local_groups = partitions.group_count[partition_index];
          const auto& row_to_group_p = partitions.row_to_group[partition_index];

          auto local_results = AggregateResults<ColumnDataType, aggregate_function>{};
          local_results.resize(local_groups);

          if (column_id == INVALID_COLUMN_ID) {
            for (auto row_index = size_t{0}; row_index < size; ++row_index) {
              ++local_results[row_to_group_p[row_index]].aggregate_count;
            }
          } else {
            const auto* const values = values_ptr->data();
            const auto* const nulls = nulls_ptr->data();

            for (auto row_index = size_t{0}; row_index < size; ++row_index) {
              const auto global_row_idx = partitions.rows[begin + row_index];
              // Skip NULL values.
              if (nulls[global_row_idx]) {
                continue;
              }
              accumulate_into_result<ColumnDataType, aggregate_function>(values[global_row_idx],
                                                                         local_results[row_to_group_p[row_index]]);
            }
          }

          fill_output_segment<ColumnDataType, aggregate_function>(local_results, local_groups, output_column_id,
                                                                  needs_null, partition_segments[partition_index]);
        });

    if (materialization_task) {
      materialization_task->set_as_predecessor_of(accumulate_job);
    }
    partitions.hash_table_ready[partition_index]->set_as_predecessor_of(accumulate_job);
    all_jobs.push_back(accumulate_job);
  }
}

/**
 * Resolves one aggregate expression's column type and window function, then dispatches into either
 * accumulate_trivial_group or accumulate_partitioned_groups.
 */
void accumulate_aggregate(const std::shared_ptr<WindowFunctionExpression>& aggregate, const size_t aggregate_index,
                          const std::shared_ptr<const Table>& input_table, const size_t groupby_column_count,
                          std::optional<GroupByPartitions>& partitions, const size_t group_count,
                          MaterializedColumnCache& column_cache, Segments& output_segments,
                          std::vector<Segments>& partition_segments, const std::vector<size_t>& chunk_row_offset,
                          std::vector<std::shared_ptr<AbstractTask>>& all_jobs,
                          TableColumnDefinitions& output_column_definitions) {
  const auto& argument_expression = aggregate->argument();
  const auto& column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(argument_expression);
  DebugAssert(column_expression, "Only column expressions are supported as aggregate arguments");
  const auto column_id = column_expression->column_id;

  const auto data_type = column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(column_id);

  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    resolve_window_function(aggregate->window_function, [&](auto window_function_t) {
      constexpr auto AGGREGATE_FUNCTION = decltype(window_function_t)::value;

      const auto [output_column_id, needs_null] = prepare_aggregate_output_column<ColumnDataType, AGGREGATE_FUNCTION>(
          aggregate, aggregate_index, input_table, column_id, groupby_column_count, output_column_definitions);

      if (!partitions) {
        accumulate_trivial_group<ColumnDataType, AGGREGATE_FUNCTION>(input_table, column_id, group_count,
                                                                     output_column_id, needs_null, output_segments);
      } else {
        accumulate_partitioned_groups<ColumnDataType, AGGREGATE_FUNCTION>(
            input_table, column_id, *partitions, column_cache, output_column_id, needs_null, chunk_row_offset,
            partition_segments, all_jobs);
      }
    });
  });
}

// Decodes one GROUP BY columns output segments (across all partitions) from the normalized key bytes.
template <typename ColumnDataType>
void write_groupby_segment(const size_t groupby_index, GroupByPartitions& partitions,
                           std::vector<Segments>& partition_segments) {
  const auto& layout = partitions.key_info.column_layouts[groupby_index];
  const auto normalized_key_size = partitions.key_info.key_size;
  const auto string_column_count = partitions.key_info.string_column_count;
  const auto partition_count = partitions.partition_count;

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(partition_count);
  for (auto partition_index = size_t{0}; partition_index < partition_count; ++partition_index) {
    if (partitions.group_count[partition_index] == 0) {
      continue;
    }
    jobs.emplace_back(std::make_shared<JobTask>([&, partition_index]() {
      const auto local_groups = partitions.group_count[partition_index];
      const auto& representative_p = partitions.group_representative_row[partition_index];

      auto values = pmr_vector<ColumnDataType>(local_groups);
      // Use uint8_t, we got race conditions with bool vector, see https://stackoverflow.com/questions/33617421/write-concurrently-vectorbool.
      auto null_flags = std::vector<uint8_t>(local_groups, 0);

      for (auto local_group_index = size_t{0}; local_group_index < local_groups; ++local_group_index) {
        const auto representative_row = representative_p[local_group_index];
        const auto* row_bytes =
            partitions.materialized_key_bytes.data() + (representative_row * normalized_key_size) +
            layout.byte_offset;

        if (row_bytes[0] == 1) {
          null_flags[local_group_index] = 1;
          continue;
        }

        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          const auto length = row_bytes[1];
          if (length > STRING_PREFIX_SIZE) {
            values[local_group_index] =
                partitions.row_strings[(representative_row * string_column_count) + layout.string_slot_index];
          } else {
            values[local_group_index] = pmr_string(reinterpret_cast<const char*>(row_bytes + 2), length);
          }
        } else {
          auto value = ColumnDataType{};
          std::memcpy(&value, row_bytes + 1, sizeof(ColumnDataType));
          values[local_group_index] = value;
        }
      }

      auto has_null = false;
      auto nulls = pmr_vector<bool>(local_groups, false);
      for (auto local_group_index = size_t{0}; local_group_index < local_groups; ++local_group_index) {
        if (null_flags[local_group_index]) {
          nulls[local_group_index] = true;
          has_null = true;
        }
      }

      if (has_null) {
        partition_segments[partition_index][groupby_index] =
            std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(nulls));
      } else {
        partition_segments[partition_index][groupby_index] =
            std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

// Decodes all GROUP BY columns output segments once every partitions local hash table is ready.
void write_groupby_output(const std::shared_ptr<const Table>& input_table,
                          const std::vector<ColumnID>& groupby_column_ids, GroupByPartitions& partitions,
                          std::vector<Segments>& partition_segments) {
  const auto groupby_column_count = groupby_column_ids.size();
  for (auto groupby_index = size_t{0}; groupby_index < groupby_column_count; ++groupby_index) {
    const auto column_id = groupby_column_ids[groupby_index];
    resolve_data_type(input_table->column_data_type(column_id), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      write_groupby_segment<ColumnDataType>(groupby_index, partitions, partition_segments);
    });
  }
}

// Fills the GROUP BY columns entries in `output_column_definitions`.
void write_groupby_column_definitions(const std::vector<ColumnID>& groupby_column_ids,
                                      const std::shared_ptr<const Table>& input_table,
                                      TableColumnDefinitions& output_column_definitions) {
  const auto groupby_column_count = groupby_column_ids.size();
  for (auto col_index = size_t{0}; col_index < groupby_column_count; ++col_index) {
    const auto col_id = groupby_column_ids[col_index];
    output_column_definitions[col_index] =
        TableColumnDefinition{input_table->column_name(col_id), input_table->column_data_type(col_id),
                              input_table->column_is_nullable(col_id)};
  }
}

// Assembles the output Table from either the single trivial chunk or one chunk per non-empty partition.
std::shared_ptr<Table> assemble_output_table(const TableColumnDefinitions& output_column_definitions,
                                             const size_t group_count, Segments& output_segments,
                                             std::optional<GroupByPartitions>& partitions,
                                             std::vector<Segments>& partition_segments) {
  auto operator_output = std::make_shared<Table>(output_column_definitions, TableType::Data);

  if (!partitions) {
    if (group_count > 0) {
      operator_output->append_chunk(output_segments);
    }
  } else {
    const auto partition_count = partitions->partition_count;
    for (auto partition_index = size_t{0}; partition_index < partition_count; ++partition_index) {
      if (partitions->group_count[partition_index] > 0) {
        operator_output->append_chunk(partition_segments[partition_index]);
      }
    }
  }
  return operator_output;
}

}  // namespace

namespace hyrise {

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  _validate_aggregates();

  const auto input_table = left_input_table();

  _output_column_definitions.resize(_groupby_column_ids.size() + _aggregates.size());
  write_groupby_column_definitions(_groupby_column_ids, input_table, _output_column_definitions);

  const auto chunk_row_offset = compute_chunk_row_offsets(input_table);

  // Collect all jobs for all aggregates, so we can schedule them at once as a single dependency graph.
  auto all_jobs = std::vector<std::shared_ptr<AbstractTask>>{};

  /**
   * Hash-partition the input rows by their GROUP BY key,
   * if there are none, all rows belong to a single implicit group.
   */
  auto group_count = size_t{1};
  auto partitions = std::optional<GroupByPartitions>{};
  if (!_groupby_column_ids.empty()) {
    partitions.emplace();
    partition_by_groupby_keys(input_table, _groupby_column_ids, chunk_row_offset, *partitions, all_jobs);
  }

  auto column_cache = MaterializedColumnCache{};
  const auto num_output_columns = _output_column_definitions.size();
  auto output_segments = Segments(num_output_columns);
  auto partition_segments =
      std::vector<Segments>(partitions ? partitions->partition_count : 0, Segments(num_output_columns));

  const auto groupby_column_count = _groupby_column_ids.size();
  auto aggregate_index = size_t{0};
  for (const auto& aggregate : _aggregates) {
    accumulate_aggregate(aggregate, aggregate_index, input_table, groupby_column_count, partitions, group_count,
                         column_cache, output_segments, partition_segments, chunk_row_offset, all_jobs,
                         _output_column_definitions);
    ++aggregate_index;
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(all_jobs);

  /**
   * Construct GROUP BY segments, decoded directly from the already-materialized key bytes/string fallback via the
   * layout info computed earlier.
   */
  if (partitions) {
    write_groupby_output(input_table, _groupby_column_ids, *partitions, partition_segments);
  }

  return assemble_output_table(_output_column_definitions, group_count, output_segments, partitions,
                               partition_segments);
}

std::shared_ptr<AbstractOperator> AggregateDYOD::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateDYOD>(copied_left_input, _aggregates, _groupby_column_ids);
}

void AggregateDYOD::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& /*parameters*/) {}

void AggregateDYOD::_on_cleanup() {}

}  // namespace hyrise
