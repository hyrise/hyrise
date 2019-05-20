#include <unordered_map>
#include <vector>

#include "all_type_variant.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/aggregate/aggregate_hashsort_aggregates.hpp"
#include "operators/aggregate/aggregate_hashsort_config.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

namespace aggregate_hashsort {

const auto data_type_size = std::unordered_map<DataType, size_t>{
    {DataType::Int, 4}, {DataType::Long, 8}, {DataType::Float, 4}, {DataType::Double, 8}};

enum class HashSortMode { Hashing, Partition };

// Data isn't copied/aggregated directly. Instead copy/aggregation operations are gathered and then executed as one.
struct TransferBufferEntry {
  size_t target_offset;
  size_t source_offset;
};

struct FixedSizeGroupRunLayout {
  FixedSizeGroupRunLayout(const size_t group_size, const std::vector<size_t>& column_base_offsets)
      : group_size(group_size), column_base_offsets(column_base_offsets) {}

  // Number of entries in `data` per group
  size_t group_size{};

  // Per GroupBy-column, its base offset in `data`.
  // I.e. the n-th entry of GroupBy-column 2 is at `data[column_base_offsets[2] + group_size * n];
  std::vector<size_t> column_base_offsets;

  size_t value_size(const ColumnID column_id) {
    if (column_id + 1 < column_base_offsets.size()) {
      return column_base_offsets[column_id + 1] - column_base_offsets[column_id];
    } else {
      return group_size - column_base_offsets.back();
    }
  }
};

struct FixedSizeGroupRun {
  using BlobDataType = uint32_t;

  FixedSizeGroupRun(const FixedSizeGroupRunLayout& layout, const size_t row_count) : layout(layout) {
    data.resize(row_count * layout.group_size);
    hashes.resize(row_count);
  }

  FixedSizeGroupRunLayout layout;

  std::vector<BlobDataType> data;

  // Hash per group
  std::vector<size_t> hashes;

  std::vector<TransferBufferEntry> transfer_buffer;

  auto value(const size_t offset) const {
    const auto begin = data.cbegin() + offset * layout.group_size;
    return std::make_pair(begin, begin + layout.group_size);
  }

  void resize(const size_t size) {
    data.resize(size * layout.group_size);
    hashes.resize(size);
  }

  template <typename T>
  void materialize_output(std::vector<T>& target_values, std::vector<bool>& target_null_values, size_t target_offset,
                          const ColumnID column_id) const {
    if (data.empty()) {
      return;
    }

    const auto nullable = !target_null_values.empty();
    const auto* source_pointer = &data[layout.column_base_offsets[column_id]];
    const auto* source_end = &data.back() + 1;

    while (source_pointer < source_end) {
      if (nullable) {
        target_null_values[target_offset] = *source_pointer != 0;
        memcpy(reinterpret_cast<char*>(&target_values[target_offset]), source_pointer + 1, sizeof(T));
      } else {
        memcpy(reinterpret_cast<char*>(&target_values[target_offset]), source_pointer, sizeof(T));
      }

      source_pointer += layout.group_size;
      ++target_offset;
    }
  }

  //  void flush_aggregation_buffer(const std::vector<std::pair<size_t, size_t>>& buffer, const FixedSizeGroupRun& source_run_segment) {
  //    for (const auto& [source_offset, target_offset] : buffer) {
  //      const auto source_value_range = source_run_segment.group_value(source_offset);
  //      std::copy(source_value_range.first, source_value_range.second, data.begin() + group_size * target_offset);
  //    }
  //  }

  size_t hash(const size_t offset) const { return hashes[offset]; }

  void copy(const size_t target_offset, const FixedSizeGroupRun& source_groups, const size_t source_offset) {
    std::copy(source_groups.data.begin() + source_offset * layout.group_size,
              source_groups.data.begin() + (source_offset + 1) * layout.group_size,
              data.begin() + target_offset * layout.group_size);
    hashes[target_offset] = source_groups.hashes[source_offset];
  }
};

//std::ostream& operator<<(std::ostream& stream, const FixedSizeGroupRun& group_run) {
//  stream << "FixedSizeGroupRun {" << std::endl;
//  stream << "  Data: " << to_string(group_run.data) << std::endl;
//  stream << "  Hashes: " << to_string(group_run.hashes) << std::endl;
//  stream << "}";
//  return stream;
//}

struct VariablySizedGroupByRunSegment {};

template <typename GroupRun>
struct Run {
  Run(GroupRun&& groups, std::vector<std::unique_ptr<BaseAggregateRun>>&& aggregates)
      : groups(std::move(groups)), aggregates(std::move(aggregates)) {}

  size_t size() const { return groups.hashes.size(); }

  GroupRun groups;
  std::vector<std::unique_ptr<BaseAggregateRun>> aggregates;

  bool is_aggregated{false};
};

template <typename GroupRun>
Run<GroupRun> make_run(const Run<GroupRun>& prototype);

template <>
Run<FixedSizeGroupRun> make_run<FixedSizeGroupRun>(const Run<FixedSizeGroupRun>& prototype) {
  auto aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>(prototype.aggregates.size());
  for (auto aggregate_idx = size_t{0}; aggregate_idx < prototype.aggregates.size(); ++aggregate_idx) {
    aggregates[aggregate_idx] = prototype.aggregates[aggregate_idx]->new_instance();
  }

  auto run = Run<FixedSizeGroupRun>{FixedSizeGroupRun{prototype.groups.layout, 0}, std::move(aggregates)};

  return run;
}

template <typename GroupRun>
struct Partition {
  std::vector<TransferBufferEntry> aggregation_buffer;
  std::vector<size_t> append_buffer;

  size_t group_key_counter{0};

  std::vector<Run<GroupRun>> runs;

  void flush_buffers(const Run<GroupRun>& input_run) {
    if (append_buffer.empty()) {
      return;
    }

    if (runs.empty()) {
      runs.emplace_back(make_run(input_run));
    }

    auto& target_run = runs.back();
    auto& target_groups = target_run.groups;

    auto target_offset = target_run.size();
    target_groups.resize(target_run.size() + append_buffer.size());
    for (const auto& source_offset : append_buffer) {
      target_groups.copy(target_offset, input_run.groups, source_offset);
      ++target_offset;
    }

    append_buffer.clear();
    aggregation_buffer.clear();
  }

  size_t size() const {
    return std::accumulate(runs.begin(), runs.end(), size_t{0},
                           [&](const auto size, const auto& run) { return size + run.size(); });
  }
};

struct Partitioning {
  size_t partition_count;
  size_t hash_shift;
  size_t hash_mask;

  Partitioning(const size_t partition_count, const size_t hash_shift, const size_t hash_mask)
      : partition_count(partition_count), hash_shift(hash_shift), hash_mask(hash_mask) {}

  size_t get_partition_index(const size_t hash) const { return (hash >> hash_shift) & hash_mask; }
};

inline Partitioning determine_partitioning(const size_t level) {
  // Best partitioning as determined by magic
  return {16, level * 4, 0b1111};
}

template <typename GroupRun>
std::vector<Partition<GroupRun>> partition(const AggregateHashSortConfig& config,
                                           std::vector<Run<GroupRun>>&& input_runs, const Partitioning& partitioning,
                                           size_t& run_idx, size_t& run_offset) {
  auto partitions = std::vector<Partition<GroupRun>>(partitioning.partition_count);

  auto counter = size_t{0};
  auto done = false;

  while (run_idx < input_runs.size()) {
    auto&& input_run = input_runs[run_idx];

    for (; run_offset < input_run.size() && !done; ++run_offset) {
      const auto partition_idx = partitioning.get_partition_index(input_run.groups.hash(run_offset));

      auto& partition = partitions[partition_idx];
      partition.append_buffer.emplace_back(run_offset);

      if (partition.append_buffer.size() > 255) {
        partition.flush_buffers(input_run);
      }

      ++counter;

      if (counter >= config.max_partitioning_counter) {
        done = true;
      }
    }

    if (run_offset >= input_run.size()) {
      ++run_idx;
      run_offset = 0;
    }

    for (auto& partition : partitions) {
      partition.flush_buffers(input_run);
    }

    if (done) {
      break;
    }
  }

  return partitions;
}

template <typename GroupRun>
std::pair<bool, std::vector<Partition<GroupRun>>> hashing(const AggregateHashSortConfig& config,
                                                          std::vector<Run<GroupRun>>&& input_runs,
                                                          const Partitioning& partitioning, size_t& run_idx,
                                                          size_t& run_offset) {
  auto partitions = std::vector<Partition<GroupRun>>(partitioning.partition_count);

  const auto hash_fn = [&](const auto& key) { return input_runs[key.first].groups.hash(key.second); };

  const auto compare_fn = [&](const auto& lhs, const auto& rhs) {
    const auto lhs_value_range = input_runs[lhs.first].groups.value(lhs.second);
    const auto rhs_value_range = input_runs[rhs.first].groups.value(rhs.second);
    return std::equal(lhs_value_range.first, lhs_value_range.second, rhs_value_range.first, rhs_value_range.second);
  };

  auto hash_table = std::unordered_map<std::pair<size_t, size_t>, size_t, decltype(hash_fn), decltype(compare_fn)>{
      config.hash_table_size, hash_fn, compare_fn};

  auto counter = size_t{0};
  auto continue_hashing = true;
  auto done = false;

  while (run_idx < input_runs.size()) {
    auto&& input_run = input_runs[run_idx];

    for (; run_offset < input_run.size() && !done; ++run_offset) {
      const auto partition_idx = partitioning.get_partition_index(input_run.groups.hash(run_offset));
      auto& partition = partitions[partition_idx];
      auto hash_table_iter = hash_table.find({run_idx, run_offset});
      if (hash_table_iter == hash_table.end()) {
        hash_table_iter = hash_table.emplace(std::pair{run_idx, run_offset}, partition.group_key_counter).first;
        partition.append_buffer.emplace_back(run_offset);
        ++partition.group_key_counter;
      }

      partition.aggregation_buffer.emplace_back(TransferBufferEntry{run_offset, hash_table_iter->second});

      if (partition.aggregation_buffer.size() > 255) {
        partition.flush_buffers(input_run);
      }

      ++counter;

      if (hash_table.load_factor() >= config.hash_table_max_load_factor) {
        if (static_cast<float>(counter) / hash_table.size() < 3) {
          continue_hashing = false;
        }

        done = true;
      }
    }

    if (run_offset >= input_run.size()) {
      ++run_idx;
      run_offset = 0;
    }

    for (auto& partition : partitions) {
      partition.flush_buffers(input_run);
    }

    if (done) {
      break;
    }
  }

  for (auto& partition : partitions) {
    for (auto& run : partition.runs) {
      run.is_aggregated = true;
    }
  }

  return {continue_hashing, std::move(partitions)};
}

template <typename GroupRun>
std::vector<Partition<GroupRun>> adaptive_hashing_and_partition(const AggregateHashSortConfig& config,
                                                                std::vector<Run<GroupRun>>&& input_runs,
                                                                const Partitioning& partitioning) {
  auto partitions = std::vector<Partition<GroupRun>>(partitioning.partition_count);

  auto mode = HashSortMode::Hashing;

  auto run_idx = size_t{0};
  auto run_offset = size_t{0};

  while (run_idx < input_runs.size()) {
    auto phase_partitions = std::vector<Partition<GroupRun>>{};

    if (mode == HashSortMode::Hashing) {
      auto [continue_hashing, hashing_partitions] =
          hashing(config, std::move(input_runs), partitioning, run_idx, run_offset);
      if (!continue_hashing) {
        mode = HashSortMode::Partition;
      }
      phase_partitions = std::move(hashing_partitions);
    } else {
      phase_partitions = partition(config, std::move(input_runs), partitioning, run_idx, run_offset);
      mode = HashSortMode::Hashing;
    }

    DebugAssert(phase_partitions.size() == partitions.size(), "");

    for (auto partition_idx = size_t{0}; partition_idx < partitioning.partition_count; ++partition_idx) {
      auto& partition = partitions[partition_idx];
      auto& phase_partition = phase_partitions[partition_idx];
      partition.runs.insert(partition.runs.end(), std::make_move_iterator(phase_partition.runs.begin()),
                            std::make_move_iterator(phase_partition.runs.end()));
    }
  }

  return partitions;
}

template <typename GroupRun>
std::vector<Run<GroupRun>> aggregate(const AggregateHashSortConfig& config, std::vector<Run<GroupRun>>&& input_runs,
                                     const size_t level) {
  if (input_runs.empty()) {
    return {};
  }

  if (input_runs.size() == 1 && input_runs.front().is_aggregated) {
    return std::move(input_runs);
  }

  auto output_runs = std::vector<Run<GroupRun>>{};

  const auto partitioning = determine_partitioning(level);

  auto partitions = adaptive_hashing_and_partition(config, std::move(input_runs), partitioning);

  for (auto&& partition : partitions) {
    if (partition.size() == 0) {
      continue;
    }

    auto aggregated_partition = aggregate(config, std::move(partition.runs), level + 1);
    output_runs.insert(output_runs.end(), std::make_move_iterator(aggregated_partition.begin()),
                       std::make_move_iterator(aggregated_partition.end()));
  }

  return output_runs;
}

template <typename SegmentPosition>
size_t hash_segment_position(const SegmentPosition& segment_position) {
  if (segment_position.is_null()) {
    return 0;
  } else {
    return boost::hash_value(segment_position.value());
  }
}

template <typename GroupRun>
GroupRun produce_initial_groups(const Table& table, const std::vector<ColumnID>& group_by_column_ids);

template <>
inline FixedSizeGroupRun produce_initial_groups<FixedSizeGroupRun>(const Table& table,
                                                                   const std::vector<ColumnID>& group_by_column_ids) {
  Assert(!group_by_column_ids.empty(), "");

  constexpr auto BLOB_DATA_TYPE_SIZE = sizeof(FixedSizeGroupRun::BlobDataType);

  /**
   * Determine layout of the `FixedSizeGroupRun::data` blob
   */
  auto group_size = size_t{};
  auto column_base_offsets = std::vector<size_t>(group_by_column_ids.size());

  for (auto output_group_by_column_id = size_t{0}; output_group_by_column_id < group_by_column_ids.size();
       ++output_group_by_column_id) {
    const auto group_column_id = group_by_column_ids[output_group_by_column_id];
    const auto group_column_size = data_type_size.at(table.column_data_type(group_column_id)) / BLOB_DATA_TYPE_SIZE +
                                   (table.column_is_nullable(group_column_id) ? 1 : 0);
    column_base_offsets[output_group_by_column_id] = group_size;
    group_size += group_column_size;
  }

  auto layout = FixedSizeGroupRunLayout{group_size, column_base_offsets};

  auto group_run = FixedSizeGroupRun{layout, table.row_count()};

  /**
   * Materialize the GroupBy-columns
   */
  auto chunk_data_base_offset = size_t{0};
  auto chunk_row_idx = size_t{0};
  for (const auto& chunk : table.chunks()) {
    for (auto output_group_by_column_idx = ColumnID{0}; output_group_by_column_idx < group_by_column_ids.size();
         ++output_group_by_column_idx) {
      auto segment_base_offset = chunk_data_base_offset + column_base_offsets[output_group_by_column_idx];
      const auto value_size = layout.value_size(output_group_by_column_idx) * BLOB_DATA_TYPE_SIZE;

      auto data_offset = segment_base_offset;
      const auto nullable = table.column_is_nullable(group_by_column_ids[output_group_by_column_idx]);
      auto row_idx = chunk_row_idx;

      segment_iterate(
          *chunk->get_segment(group_by_column_ids[output_group_by_column_idx]), [&](const auto& segment_position) {
            if (segment_position.is_null()) {
              group_run.data[data_offset] = 1;
              memset(&group_run.data[data_offset + 1], 0, value_size - BLOB_DATA_TYPE_SIZE);
            } else {
              if (nullable) {
                group_run.data[data_offset] = 0;
                memcpy(&group_run.data[data_offset + 1], &segment_position.value(), value_size - BLOB_DATA_TYPE_SIZE);
              } else {
                memcpy(&group_run.data[data_offset], &segment_position.value(), value_size);
              }
            }

            boost::hash_combine(group_run.hashes[row_idx], hash_segment_position(segment_position));

            data_offset += layout.group_size;
            ++row_idx;
          });
    }

    chunk_row_idx += chunk->size();
    chunk_data_base_offset += layout.group_size * chunk->size();
  }

  return group_run;
}

}  // namespace aggregate_hashsort

}  // namespace opossum
