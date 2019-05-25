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

const auto data_type_sizes = std::unordered_map<DataType, size_t>{
    {DataType::Int, 4}, {DataType::Long, 8}, {DataType::Float, 4}, {DataType::Double, 8}};

enum class HashSortMode { Hashing, Partition };

struct FixedSizeGroupRunLayout {
  FixedSizeGroupRunLayout(const size_t group_size, const std::vector<size_t>& column_base_offsets)
      : group_size(group_size), column_base_offsets(column_base_offsets) {}

  // Number of entries in `data` per group
  size_t group_size{};

  // Per GroupBy-column, its base offset in `data`.
  // I.e. the n-th entry of GroupBy-column 2 is at `data[column_base_offsets[2] + group_size * n];
  std::vector<size_t> column_base_offsets;

  size_t value_size(const ColumnID column_id) const {
    if (column_id + 1 < column_base_offsets.size()) {
      return column_base_offsets[column_id + 1] - column_base_offsets[column_id];
    } else {
      return group_size - column_base_offsets.back();
    }
  }
};

struct FixedSizeGroupRun {
  using BlobDataType = uint32_t;
  using LayoutType = FixedSizeGroupRunLayout;

  FixedSizeGroupRun(const FixedSizeGroupRunLayout* layout) : layout(layout) {}

  const FixedSizeGroupRunLayout* layout;

  std::vector<BlobDataType> data;

  // Hash per group
  std::vector<size_t> hashes;

  bool compare(const size_t own_offset, const FixedSizeGroupRun& other_run, const size_t other_offset) const {
    const auto own_begin = data.cbegin() + own_offset * layout->group_size;
    const auto other_begin = other_run.data.cbegin() + other_offset * layout->group_size;
    return std::equal(own_begin, own_begin + layout->group_size, other_begin, other_begin + layout->group_size);
  }

  template <typename T>
  std::shared_ptr<BaseSegment> materialize_output(const ColumnID column_id, const bool nullable) const {
    const auto* source_pointer = &data[layout->column_base_offsets[column_id]];
    const auto* source_end = &data.back() + 1;

    auto target_offset = size_t{0};

    auto values = std::vector<T>(hashes.size());
    auto null_values = std::vector<bool>(nullable ? hashes.size() : 0u);

    while (source_pointer < source_end) {
      if (nullable) {
        null_values[target_offset] = *source_pointer != 0;
        memcpy(reinterpret_cast<char*>(&values[target_offset]), source_pointer + 1, sizeof(T));
      } else {
        memcpy(reinterpret_cast<char*>(&values[target_offset]), source_pointer, sizeof(T));
      }

      source_pointer += layout->group_size;
      ++target_offset;
    }

    if (nullable) {
      return std::make_shared<ValueSegment<T>>(std::move(values), std::move(null_values));
    } else {
      return std::make_shared<ValueSegment<T>>(std::move(values));
    }
  }

  void flush_append_buffer(const std::vector<size_t>& buffer, const FixedSizeGroupRun& source) {
    auto target_offset = hashes.size();
    resize(hashes.size() + buffer.size());
    for (const auto& source_offset : buffer) {
      std::copy(source.data.begin() + source_offset * layout->group_size,
                source.data.begin() + (source_offset + 1) * layout->group_size,
                data.begin() + target_offset * layout->group_size);
      hashes[target_offset] = source.hashes[source_offset];
      ++target_offset;
    }
  }

  size_t hash(const size_t offset) const { return hashes[offset]; }

  void resize(const size_t size) {
    data.resize(size * layout->group_size);
    hashes.resize(size);
  }
};

struct VariablySizedGroupRunLayout {
  std::vector<ColumnID> variably_sized_column_ids;
  std::vector<ColumnID> fixed_size_column_ids;

  // Mapping a ColumnID (the index of the vector) to whether the column's values is stored either in the
  // VariablySizeGroupRun itself, or the FixedSizeGroupRun and the value index in the respective group run.
  using Column = std::tuple<bool /* is_variably_sized */, size_t /* index */, std::optional<size_t> /* nullable_index */
                            >;
  std::vector<Column> column_mapping;

  FixedSizeGroupRunLayout fixed_layout;

  VariablySizedGroupRunLayout(const std::vector<ColumnID>& variably_sized_column_ids, const std::vector<ColumnID>& fixed_size_column_ids, const std::vector<Column>& column_mapping,
                              const FixedSizeGroupRunLayout& fixed_layout)
      :
        variably_sized_column_ids(variably_sized_column_ids),
        fixed_size_column_ids(fixed_size_column_ids),
        column_mapping(column_mapping),
        fixed_layout(fixed_layout) {}
};

struct VariablySizedGroupRun {
  using BlobDataType = uint32_t;
  using LayoutType = VariablySizedGroupRunLayout;

  const VariablySizedGroupRunLayout* layout;

  std::vector<BlobDataType> data;
  std::vector<bool> null_values;

  // End indices in `data`
  std::vector<size_t> group_end_offsets;

  // Offsets (in bytes) for all values in all groups, relative to the beginning of the group.
  std::vector<size_t> value_end_offsets;

  // Non-variably sized values in the group and hash values
  FixedSizeGroupRun fixed;

  explicit VariablySizedGroupRun(const VariablySizedGroupRunLayout* layout)
      : layout(layout), fixed(&layout->fixed_layout) {}

  bool compare(const size_t own_offset, const FixedSizeGroupRun& other_run, const size_t other_offset) const {
    if (!fixed.compare(own_offset, other_run, other_offset)) {
      return false;
    }

    return false;
  }

  template <typename T>
  std::shared_ptr<BaseSegment> materialize_output(const ColumnID column_id, const bool nullable) const {
    return nullptr;
  }

  void flush_append_buffer(const std::vector<size_t>& buffer, const FixedSizeGroupRun& source) {}

  size_t hash(const size_t offset) const { return fixed.hash(offset); }
};

template <typename GroupRun>
struct Run {
  Run(GroupRun&& groups, std::vector<std::unique_ptr<BaseAggregateRun>>&& aggregates)
      : groups(std::move(groups)), aggregates(std::move(aggregates)) {}

  size_t size() const { return groups.hashes.size(); }

  Run<GroupRun> new_instance() const {
    auto new_aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>(this->aggregates.size());
    for (auto aggregate_idx = size_t{0}; aggregate_idx < new_aggregates.size(); ++aggregate_idx) {
      new_aggregates[aggregate_idx] = this->aggregates[aggregate_idx]->new_instance();
    }

    return Run<FixedSizeGroupRun>{FixedSizeGroupRun{groups.layout}, std::move(new_aggregates)};
  }

  GroupRun groups;
  std::vector<std::unique_ptr<BaseAggregateRun>> aggregates;

  bool is_aggregated{false};
};

template <typename GroupRun>
struct Partition {
  std::vector<AggregationBufferEntry> aggregation_buffer;
  std::vector<size_t> append_buffer;

  size_t group_key_counter{0};

  std::vector<Run<GroupRun>> runs;

  void flush_buffers(const Run<GroupRun>& input_run) {
    if (append_buffer.empty() && aggregation_buffer.empty()) {
      return;
    }

    if (runs.empty()) {
      runs.emplace_back(input_run.new_instance());
    }

    auto& target_run = runs.back();
    auto& target_groups = target_run.groups;

    /**
     * Process append_buffer
     */
    target_groups.flush_append_buffer(append_buffer, input_run.groups);

    for (auto aggregate_idx = size_t{0}; aggregate_idx < target_run.aggregates.size(); ++aggregate_idx) {
      auto& target_aggregate_run = target_run.aggregates[aggregate_idx];
      const auto& input_aggregate_run = input_run.aggregates[aggregate_idx];
      target_aggregate_run->flush_append_buffer(append_buffer, *input_aggregate_run);
      target_aggregate_run->flush_aggregation_buffer(aggregation_buffer, *input_aggregate_run);
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
    return input_runs[lhs.first].groups.compare(lhs.second, input_runs[rhs.first].groups, rhs.second);
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
      } else {
        partition.aggregation_buffer.emplace_back(AggregationBufferEntry{hash_table_iter->second, run_offset});
      }

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

template <typename GroupRunLayout>
GroupRunLayout produce_initial_groups_layout(const Table& table, const std::vector<ColumnID>& group_by_column_ids);

template <>
FixedSizeGroupRunLayout produce_initial_groups_layout<FixedSizeGroupRunLayout>(
    const Table& table, const std::vector<ColumnID>& group_by_column_ids) {
  constexpr auto BLOB_DATA_TYPE_SIZE = sizeof(FixedSizeGroupRun::BlobDataType);

  /**
   * Determine layout of the `FixedSizeGroupRun::data` blob
   */
  auto group_size = size_t{};
  auto column_base_offsets = std::vector<size_t>(group_by_column_ids.size());

  for (auto output_group_by_column_id = size_t{0}; output_group_by_column_id < group_by_column_ids.size();
       ++output_group_by_column_id) {
    const auto group_column_id = group_by_column_ids[output_group_by_column_id];
    const auto group_column_size = data_type_sizes.at(table.column_data_type(group_column_id)) / BLOB_DATA_TYPE_SIZE +
                                   (table.column_is_nullable(group_column_id) ? 1 : 0);
    column_base_offsets[output_group_by_column_id] = group_size;
    group_size += group_column_size;
  }

  return FixedSizeGroupRunLayout{group_size, column_base_offsets};
}

template <>
VariablySizedGroupRunLayout produce_initial_groups_layout<VariablySizedGroupRunLayout>(
    const Table& table, const std::vector<ColumnID>& group_by_column_ids) {
  Assert(!group_by_column_ids.empty(), "");

  auto column_mapping = std::vector<VariablySizedGroupRunLayout::Column>(group_by_column_ids.size());
  auto nullable_count = size_t{0};
  auto variably_sized_column_ids = std::vector<ColumnID>{};
  auto fixed_size_column_ids = std::vector<ColumnID>{};

  for (auto output_group_by_column_id = ColumnID{0}; output_group_by_column_id < group_by_column_ids.size();
       ++output_group_by_column_id) {
    const auto group_by_column_id = group_by_column_ids[output_group_by_column_id];
    const auto data_type = table.column_data_type(group_by_column_id);

    if (data_type == DataType::String) {
      if (table.column_is_nullable(group_by_column_id)) {
        column_mapping[output_group_by_column_id] = {true, variably_sized_column_ids.size(), nullable_count};
        ++nullable_count;
      } else {
        column_mapping[output_group_by_column_id] = {true, variably_sized_column_ids.size(), std::nullopt};
      }
      variably_sized_column_ids.emplace_back(group_by_column_id);
    } else {
      column_mapping[output_group_by_column_id] = {false, fixed_size_column_ids.size(), std::nullopt};
      fixed_size_column_ids.emplace_back(group_by_column_id);
    }
  }

  const auto fixed_layout =
      produce_initial_groups_layout<FixedSizeGroupRunLayout>(table, fixed_size_column_ids);

  return VariablySizedGroupRunLayout{variably_sized_column_ids, fixed_size_column_ids, column_mapping, fixed_layout};
}

template <typename GroupRun>
GroupRun produce_initial_groups(const Table& table, const typename GroupRun::LayoutType* layout,
                                const std::vector<ColumnID>& group_by_column_ids);

template <>
inline FixedSizeGroupRun produce_initial_groups<FixedSizeGroupRun>(const Table& table,
                                                                   const FixedSizeGroupRunLayout* layout,
                                                                   const std::vector<ColumnID>& group_by_column_ids) {
  constexpr auto BLOB_DATA_TYPE_SIZE = sizeof(FixedSizeGroupRun::BlobDataType);

  auto group_run = FixedSizeGroupRun{layout};
  group_run.resize(table.row_count());

  /**
   * Materialize the GroupBy-columns
   */
  auto chunk_data_base_offset = size_t{0};
  auto chunk_row_idx = size_t{0};
  for (const auto& chunk : table.chunks()) {
    for (auto output_group_by_column_idx = ColumnID{0}; output_group_by_column_idx < group_by_column_ids.size();
         ++output_group_by_column_idx) {
      auto segment_base_offset = chunk_data_base_offset + layout->column_base_offsets[output_group_by_column_idx];
      const auto value_size = layout->value_size(output_group_by_column_idx) * BLOB_DATA_TYPE_SIZE;

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

            data_offset += layout->group_size;
            ++row_idx;
          });
    }

    chunk_row_idx += chunk->size();
    chunk_data_base_offset += layout->group_size * chunk->size();
  }

  return group_run;
}

template <>
inline VariablySizedGroupRun produce_initial_groups<VariablySizedGroupRun>(
    const Table& table, const VariablySizedGroupRunLayout* layout, const std::vector<ColumnID>& group_by_column_ids) {
  Assert(!layout->variably_sized_column_ids.empty(), "");

  constexpr auto BLOB_DATA_TYPE_SIZE = sizeof(VariablySizedGroupRun::BlobDataType);
  //constexpr auto SIZE_T_SIZE = sizeof(size_t) / BLOB_DATA_TYPE_SIZE;

  const auto row_count = table.row_count();
  const auto column_count = layout->variably_sized_column_ids.size();

  const auto nullable_column_count = std::count_if(layout->variably_sized_column_ids.begin(), layout->variably_sized_column_ids.end(), [&](const ColumnID column_id) {
    return table.column_is_nullable(column_id);
  });

  auto null_values = std::vector<bool>(row_count * nullable_column_count);
  auto nullable_column_idx = size_t{0};

  auto value_end_offsets = std::vector<size_t>(row_count * layout->variably_sized_column_ids.size());

  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    const auto group_by_column_id = layout->variably_sized_column_ids[column_id];
    const auto nullable = table.column_is_nullable(group_by_column_id);

    auto null_values_iter = nullable ? null_values.begin() + nullable_column_idx : null_values.end();
    auto value_end_offsets_iter = value_end_offsets.begin() + column_id;

    auto column_iterable = ColumnIterable{table, group_by_column_id};
    column_iterable.for_each<pmr_string>([&](const auto& segment_position, const auto offset) {
      const auto previous_value_end = column_id == 0 ? 0 : *(value_end_offsets_iter - 1);
      *value_end_offsets_iter = previous_value_end + (segment_position.is_null() ? 0 : segment_position.value().size());
      value_end_offsets_iter += column_count;

      if (nullable) {
        *null_values_iter = segment_position.is_null();
        null_values_iter += nullable_column_count;
      }
    });

    if (nullable) {
      ++nullable_column_idx;
    }
  }

  auto group_end_offsets = std::vector<size_t>(row_count);
  auto previous_group_end_offset = size_t{0};
  auto value_end_offset_iter = value_end_offsets.begin() + (column_count - 1);
  for (auto row_idx = size_t{0}; row_idx < row_count; ++row_idx) {
    // Round up to a full BLOB_DATA_TYPE_SIZE
    const auto group_element_count = *value_end_offset_iter / BLOB_DATA_TYPE_SIZE + (*value_end_offset_iter % BLOB_DATA_TYPE_SIZE > 0 ? 1 : 0);

    previous_group_end_offset += group_element_count;

    group_end_offsets[row_idx] = previous_group_end_offset;
    value_end_offset_iter += column_count;
  }

  const auto element_count = group_end_offsets.back();
  auto group_data = std::vector<VariablySizedGroupRun::BlobDataType>(element_count);

  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    auto column_iterable = ColumnIterable{table, layout->variably_sized_column_ids[column_id]};
    column_iterable.for_each<pmr_string>([&](const auto& segment_position, const auto offset) {
      if (segment_position.is_null()) {
        return;
      }

      const auto group_begin_offset = offset == 0 ? 0 : group_end_offsets[offset - 1];
      const auto value_begin_offset = column_id == 0 ? 0 : value_end_offsets[column_id - 1];

      auto* target = &reinterpret_cast<char*>(&group_data[group_begin_offset])[value_begin_offset];

      memcpy(target, segment_position.value().data(), segment_position.value().size());
    });
  }

  auto groups = VariablySizedGroupRun{layout};
  groups.data = std::move(group_data);
  groups.null_values = std::move(null_values);
  groups.group_end_offsets = std::move(group_end_offsets);
  groups.value_end_offsets = std::move(value_end_offsets);
  groups.fixed = produce_initial_groups<FixedSizeGroupRun>(table, &layout->fixed_layout, layout->fixed_size_column_ids);

  return groups;
}

}  // namespace aggregate_hashsort

}  // namespace opossum
