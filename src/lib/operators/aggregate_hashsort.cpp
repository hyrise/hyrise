#include "aggregate_hashsort.hpp"

#include "boost/functional/hash.hpp"

#include "storage/segment_iterate.hpp"
#include "operators/aggregate/aggregate_traits.hpp"

namespace {

using namespace opossum;  // NOLINT
//
//template<typename T>
//std::string to_string(const std::vector<T>& data) {
//  std::stringstream s;
//  for (const auto& v : data) s << v << " ";
//  return s.str();
//}

const auto data_type_size = std::unordered_map<DataType, size_t>{
  {DataType::Int, 4},
  {DataType::Long, 8},
  {DataType::Float, 4},
  {DataType::Double, 8}
};

enum class HashSortMode {
  Hashing, Partitioning
};

struct ColumnIterable {
  const Table& table;
  const ColumnID column_id;

  template<typename T, typename F>
  void for_each(const F& f) const {
    auto offset = size_t{0};

    for (const auto& chunk : table.chunks()) {
      const auto &segment = *chunk->get_segment(column_id);

      segment_with_iterators<T>(segment, [&](auto begin, auto end) {
        std::for_each(begin, end, [&](const auto& segment_position) {
          f(segment_position, offset);
          ++offset;
        });
      });
    }
  }
};

struct BaseColumnMaterialization {
  virtual ~BaseColumnMaterialization() = default;
};

template<typename T>
struct ColumnMaterialization : public BaseColumnMaterialization {
  std::vector<T> values;
  std::vector<bool> null_values;
};

struct BaseRunSegment {
  virtual ~BaseRunSegment() = default;

  virtual void resize(const size_t size) = 0;
  virtual std::unique_ptr<BaseRunSegment> new_instance() const = 0;
  virtual void flush_aggregation_buffer(const std::vector<std::pair<size_t, size_t>>& buffer, const BaseRunSegment& base_source_run_segment) = 0;
  virtual void materialize(BaseColumnMaterialization& target, const size_t offset) const = 0;
};

// Data isn't copied/aggregated directly. Instead copy/aggregation operations are gathered and then executed as one.
struct TransferBufferEntry {
  size_t target_offset;
  size_t source_offset;
};

struct FixedSizeGroupRunLayout {
  // Number of entries in `data` per group
  size_t group_size{};

  // Per GroupBy-column, its base offset in `data`.
  // I.e. the n-th entry of GroupBy-column 2 is at `data[column_base_offsets[2] + group_size * n];
  std::vector<size_t> column_base_offsets;

  size_t value_size(const ColumnID column_id) {
    if (column_id + 1< column_base_offsets.size()) {
      return column_base_offsets[column_id + 1] - column_base_offsets[column_id];
    } else {
      return group_size - column_base_offsets.back();
    }
  }
};

struct FixedSizeGroupRun {
  using BlobDataType = uint32_t;

  FixedSizeGroupRun(const FixedSizeGroupRunLayout& layout, const size_t row_count):
    layout(layout) {
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

  template<typename T>
  void materialize_output(std::vector<T>& target_values, std::vector<bool>& target_null_values, size_t target_offset, const ColumnID column_id) const {
    if (data.empty()) {
      return;
    }

    const auto nullable = !target_null_values.empty();
    const auto *source_pointer = &data[layout.column_base_offsets[column_id]];
    const auto *source_end = &data.back() + 1;

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

  size_t hash(const size_t offset) const {
    return hashes[offset];
  }

  void copy(const size_t target_offset, const FixedSizeGroupRun& source_groups, const size_t source_offset) {
    std::copy(source_groups.data.begin() + source_offset * layout.group_size, source_groups.data.begin() + (source_offset + 1) * layout.group_size, data.begin() + target_offset * layout.group_size);
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

template<typename SourceColumnDataType>
struct SumRunSegment : public BaseRunSegment {
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Sum>::AggregateType;

  SumRunSegment() = default;

  SumRunSegment(const ColumnIterable& column_iterable) {
    sums.resize(column_iterable.table.row_count());

    column_iterable.for_each<SourceColumnDataType>([&](const auto segment_position, const auto offset) {
      if (segment_position.is_null()) {
        return;
      } else {
        sums[offset] = segment_position.value();
      }
    });

  }

  void resize(const size_t size) override {
    sums.resize(size);
  }

  void flush_aggregation_buffer(const std::vector<std::pair<size_t, size_t>>& buffer, const BaseRunSegment& base_source_run_segment) override {
    const auto& source_run_segment = static_cast<const SumRunSegment<SourceColumnDataType>&>(base_source_run_segment);

    for (const auto& [source_offset, target_offset] : buffer) {
      const auto& source_sum = source_run_segment.sums[source_offset];
      auto& target_sum = sums[target_offset];

      if (!source_sum) continue;

      if (!target_sum) {
        target_sum = source_sum;
        continue;
      }

      *target_sum += *source_sum;
    }
  }

  std::unique_ptr<BaseRunSegment> new_instance() const override {
    return std::make_unique<SumRunSegment>();
  }

  void materialize(BaseColumnMaterialization& base_target, size_t target_offset) const override {
    auto& target = static_cast<ColumnMaterialization<AggregateType>&>(base_target);

    for (auto source_offset = size_t{0}; source_offset < sums.size(); ++source_offset, ++target_offset) {
      target.values[target_offset] = sums[source_offset].value_or(0);
      target.null_values[target_offset] = !sums[source_offset].has_value();
    }
  }

  std::vector<std::optional<AggregateType>> sums;
};

template<typename GroupRun>
struct Run {
  Run(const size_t size, GroupRun&& groups, std::vector<std::unique_ptr<BaseRunSegment>>&& aggregates):
    groups(std::move(groups)), aggregates(std::move(aggregates)), size(size) {}

  bool is_aggregated{false};
  
  GroupRun groups;
  std::vector<std::unique_ptr<BaseRunSegment>> aggregates;
  
  size_t size{0};
};

template<typename GroupRun>
Run<GroupRun> make_run(const Run<GroupRun>& prototype);

template<>
Run<FixedSizeGroupRun> make_run<FixedSizeGroupRun>(const Run<FixedSizeGroupRun>& prototype) {
  auto aggregates = std::vector<std::unique_ptr<BaseRunSegment>>(prototype.aggregates.size());
  for (auto aggregate_idx = size_t{0}; aggregate_idx < prototype.aggregates.size(); ++aggregate_idx) {
    aggregates[aggregate_idx] = prototype.aggregates[aggregate_idx]->new_instance();
  }

  auto run = Run<FixedSizeGroupRun>{
  0,
  FixedSizeGroupRun{prototype.groups.layout, 0},
  std::move(aggregates)
  };

  return run;
}

template<typename GroupRun>
struct Partition {
  std::vector<TransferBufferEntry> aggregation_buffer;
  std::vector<size_t> append_buffer;

  size_t group_key_counter{0};

  std::vector<Run<GroupRun>> runs;

  void flush_buffers(const Run<GroupRun>& input_run) {
    if (runs.empty()) {
      runs.emplace_back(make_run(input_run));
    }

    auto& target_run = runs.back();
    auto& target_groups = target_run.groups;

    auto target_offset = target_run.size;
    target_groups.resize(target_run.size + append_buffer.size());
    for (const auto& source_offset : append_buffer) {
      target_groups.copy(target_offset, input_run.groups, source_offset);
      ++target_offset;
    }

    target_run.size += append_buffer.size();

    append_buffer.clear();
    aggregation_buffer.clear();
  }

  size_t size() const {
    return std::accumulate(runs.begin(), runs.end(), size_t{0}, [&](const auto size, const auto& run) {
      return size + run.size;
    });
  }
};

//template<typename GroupRun>
//void append(GroupRun& target, const GroupRun& source, const size_t offset) {
//  const auto source_value_range = source.group_value(offset);
//  target.data.insert(target.data.end(), source_value_range.first, source_value_range.second);
//}
//
//template<typename GroupRun>
//void flush_aggregation_buffer(Partition<GroupRun>& partition, const Run<GroupRun>& source_run) {
//  if (partition.aggregation_buffer.empty()) return;
//
//  const auto max_group_key = std::max_element(partition.aggregation_buffer.begin(), partition.aggregation_buffer.end(), [](const auto& a, const auto& b) {
//    return a.second < b.second;
//  });
//
//  auto& target_run = partition.runs.back();
//
//  if (max_group_key->second >= target_run.size) {
//    target_run.groups.resize(max_group_key->second + 1);
//    for (auto& aggregate_run_segment : target_run.aggregates) {
//      aggregate_run_segment->resize(max_group_key->second + 1);
//    }
//    target_run.size = max_group_key->second + 1;
//  }
//
//  target_run.groups.flush_aggregation_buffer(partition.aggregation_buffer, source_run.groups);
//
//  for (auto aggregate_idx = size_t{0}; aggregate_idx < target_run.aggregates.size(); ++aggregate_idx) {
//    target_run.aggregates[aggregate_idx]->flush_aggregation_buffer(partition.aggregation_buffer, *source_run.aggregates[aggregate_idx]);
//  }
//
//  partition.aggregation_buffer.clear();
//}

struct Partitioning {
  size_t partition_count;
  size_t hash_shift;
  size_t hash_mask;

  size_t get_partition_index(const size_t hash) const {
    return (hash >> hash_shift) & hash_mask;
  }
};

Partitioning determine_partitioning(const size_t level) {
  return {8, level * 3, 0b111};
}


template<typename GroupRun>
std::vector<Partition<GroupRun>> partition(std::vector<Run<GroupRun>>&& input_runs, const Partitioning& partitioning, size_t& run_idx, size_t& run_offset) {
  auto partitions = std::vector<Partition<GroupRun>>(partitioning.partition_count);

  auto counter = size_t{0};
  auto done = false;

  for (; run_idx < input_runs.size(); ++run_idx) {
    auto&& input_run = input_runs[run_idx];

    for (; run_offset < input_run.size && !done; ++run_offset) {
      const auto partition_idx = partitioning.get_partition_index(input_run.groups.hash(run_offset));

      auto &partition = partitions[partition_idx];
      partition.append_buffer.emplace_back(run_offset);

      if (partition.append_buffer.size() > 255) {
        partition.flush_buffers(input_run);
      }

      if (counter > 50'000) {
        done = true;
      }
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

template<typename GroupRun>
std::vector<Partition<GroupRun>> hashing(std::vector<Run<GroupRun>>&& input_runs, const Partitioning& partitioning, size_t& run_idx, size_t& run_offset) {
  auto partitions = std::vector<Partition<GroupRun>>(partitioning.partition_count);

  const auto hash_fn = [&](const auto& key) {
    return input_runs[key.first].groups.hash(key.second);
  };

  const auto compare_fn = [&](const auto& lhs, const auto& rhs) {
    const auto lhs_value_range = input_runs[lhs.first].groups.value(lhs.second);
    const auto rhs_value_range = input_runs[rhs.first].groups.value(rhs.second);
    return std::equal(lhs_value_range.first, lhs_value_range.second, rhs_value_range.first, rhs_value_range.second);
  };

  auto hash_table = std::unordered_map<std::pair<size_t, size_t>, size_t, decltype(hash_fn), decltype(compare_fn)>{10'000, hash_fn, compare_fn};

  auto counter = size_t{0};
  auto done = false;

  for (; run_idx < input_runs.size(); ++run_idx) {
    auto&& input_run = input_runs[run_idx];

    for (; run_offset < input_run.size && !done; ++run_offset) {
      const auto partition_idx = partitioning.get_partition_index(input_run.groups.hash(run_offset));
      auto &partition = partitions[partition_idx];
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

      if (hash_table.load_factor() > 0.25) {
        if (static_cast<float>(counter) / hash_table.size() < 3) {
          done = true;
        }

        hash_table.clear();
      }
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

template<typename GroupRun>
std::vector<Run<GroupRun>> aggregate(std::vector<Run<GroupRun>>&& input_runs, const size_t level) {
  if (input_runs.empty()) {
    return {};
  }

  if (input_runs.size() == 1 && input_runs.front().is_aggregated) {
    return std::move(input_runs);
  }

  const auto partitioning = determine_partitioning(level);

  auto partitions = std::vector<Partition<GroupRun>>(partitioning.partition_count);
  for (auto& partition : partitions) {
    partition.runs.emplace_back(make_run(input_runs.front()));
  }

  const auto hash_fn = [&](const auto& key) {
    return input_runs[key.first].groups.hash(key.second);
  };

  const auto compare_fn = [&](const auto& lhs, const auto& rhs) {
    const auto lhs_value_range = input_runs[lhs.first].groups.value(lhs.second);
    const auto rhs_value_range = input_runs[rhs.first].groups.value(rhs.second);
    return std::equal(lhs_value_range.first, lhs_value_range.second, rhs_value_range.first, rhs_value_range.second);
  };

  auto hash_table = std::unordered_map<std::pair<size_t, size_t>, size_t, decltype(hash_fn), decltype(compare_fn)>{3'000, hash_fn, compare_fn};

  auto mode = HashSortMode::Hashing;

  for (auto run_idx = size_t{0}; run_idx < input_runs.size(); ++run_idx) {
    auto&& input_run = input_runs[run_idx];

    for (auto run_offset = size_t{0}; run_offset < input_run.size;) {
      if (mode == HashSortMode::Hashing) {
        for (; run_offset < input_run.size; ++run_offset) {
          const auto partition_idx = partitioning.get_partition_index(input_run.groups.hash(run_offset));
          auto &partition = partitions[partition_idx];
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

          if (hash_table.load_factor() > 0.25) {
            mode = HashSortMode::Partitioning;

            hash_table.clear();
          }
        }
      } else {
        for (; run_offset < input_run.size; ++run_offset) {
          const auto partition_idx = partitioning.get_partition_index(input_run.groups.hash(run_offset));
          // //std::cout << "Partition (" << input_run.groups.hash(run_offset) << ")" << run_offset << " -> " << partition_idx << std::endl;
          auto &partition = partitions[partition_idx];
          partition.append_buffer.emplace_back(run_offset);

          if (partition.append_buffer.size() > 255) {
            partition.flush_buffers(input_run);
          }
        }
      }
    }

    for (auto& partition : partitions) {
      partition.flush_buffers(input_run);
    }
  }

  for (auto& partition : partitions) {
    if (!partition.runs.empty() && partition.runs.back().size == 0) {
      partition.runs.pop_back();
    }
  }

  auto output_runs = std::vector<Run<GroupRun>>{};

  for (auto&& partition : partitions) {
    if (partition.size() == 0) {
      continue;
    }

    // //std::cout << "Level " << level << " Rows: " << partition.size() << std::endl;
    auto aggregated_partition = aggregate(std::move(partition.runs), level + 1);
    output_runs.insert(output_runs.end(), std::make_move_iterator(aggregated_partition.begin()), std::make_move_iterator(aggregated_partition.end()));
  }

  return output_runs;
}

std::vector<std::unique_ptr<BaseRunSegment>> produce_initial_aggregates(const Table& table, const std::vector<AggregateColumnDefinition>& aggregate_column_definitions) {
  auto aggregates = std::vector<std::unique_ptr<BaseRunSegment>>(aggregate_column_definitions.size());
  for (auto aggregate_idx = size_t{0}; aggregate_idx < aggregates.size(); ++aggregate_idx) {
    const auto& aggregate_column_definition = aggregate_column_definitions[aggregate_idx];

    if (!aggregate_column_definition.column) {
      Fail("Nye");
    }

    const auto source_column_id = *aggregate_column_definition.column;

    resolve_data_type(table.column_data_type(*aggregate_column_definition.column), [&](const auto data_type_t) {
      using SourceColumnDataType = typename decltype(data_type_t)::type;

      ColumnIterable column_iterable{table, source_column_id};

      if constexpr (std::is_same_v<SourceColumnDataType, pmr_string>) {
        Fail("ajkhn");
      } else {
        switch (aggregate_column_definition.function) {
          case AggregateFunction::Min:
            aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>(column_iterable);
            break;
          case AggregateFunction::Max:
            aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>(column_iterable);
            break;
          case AggregateFunction::Sum:
            aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>(column_iterable);
            break;
          case AggregateFunction::Avg:
            aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>(column_iterable);
            break;
          case AggregateFunction::Count:
            aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>(column_iterable);
            break;
          case AggregateFunction::CountDistinct:
            aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>(column_iterable);
            break;
        }
      }
    });
  }

  return aggregates;
}

template<typename SegmentPosition>
size_t hash_segment_position(const SegmentPosition& segment_position) {
  if (segment_position.is_null()) {
    return 0;
  } else {
    return boost::hash_value(segment_position.value());
  }
}

template<typename GroupRun>
GroupRun produce_initial_group_run(const Table& table, const std::vector<ColumnID>& group_by_column_ids);

template<>
FixedSizeGroupRun produce_initial_group_run<FixedSizeGroupRun>(const Table& table, const std::vector<ColumnID>& group_by_column_ids) {
  Assert(!group_by_column_ids.empty(), "");

  constexpr auto BLOB_DATA_TYPE_SIZE = sizeof(FixedSizeGroupRun::BlobDataType);

  /**
   * Determine layout of the `FixedSizeGroupRun::data` blob
   */
  auto group_size = size_t{};
  auto column_base_offsets = std::vector<size_t>(group_by_column_ids.size());

  for (auto output_group_by_column_id = size_t{0}; output_group_by_column_id < group_by_column_ids.size(); ++output_group_by_column_id) {
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
    for (auto output_group_by_column_idx = ColumnID{0};  output_group_by_column_idx < group_by_column_ids.size(); ++output_group_by_column_idx) {
      auto segment_base_offset = chunk_data_base_offset + column_base_offsets[output_group_by_column_idx];
      const auto value_size = layout.value_size(output_group_by_column_idx) * BLOB_DATA_TYPE_SIZE;

      auto data_offset = segment_base_offset;
      const auto nullable = table.column_is_nullable(group_by_column_ids[output_group_by_column_idx]);
      auto row_idx = chunk_row_idx;

      segment_iterate(*chunk->get_segment(group_by_column_ids[output_group_by_column_idx]), [&](const auto &segment_position) {
        if (segment_position.is_null()) {
          group_run.data[data_offset] = 1;
          memset(&group_run.data[data_offset + 1], 0, value_size - BLOB_DATA_TYPE_SIZE);
        } else {
          if (nullable) {
            group_run.data[data_offset] = 0;
            memcpy(&group_run.data[data_offset + 1], &segment_position.value(),
                   value_size - BLOB_DATA_TYPE_SIZE);
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

//  //std::cout << "BaseOffsets: " << to_string(column_base_offsets) << std::endl;
//  //std::cout << "Data: " << to_string(group_run.data) << std::endl;
//  //std::cout << "Hashes: " << to_string(group_run.hashes) << std::endl;

  return group_run;
}

}  // namespace

namespace opossum {

AggregateHashSort::AggregateHashSort(const std::shared_ptr<AbstractOperator> &in,
                                     const std::vector<AggregateColumnDefinition> &aggregates,
                                     const std::vector<ColumnID> &groupby_column_ids)
: AbstractAggregateOperator(in, aggregates, groupby_column_ids) {}

const std::string AggregateHashSort::name() const { return "AggregateHashSort"; }

std::shared_ptr<AbstractOperator> AggregateHashSort::_on_deep_copy(
const std::shared_ptr<AbstractOperator> &copied_input_left,
const std::shared_ptr<AbstractOperator> &copied_input_right) const {
  return std::make_shared<AggregateHashSort>(copied_input_left, _aggregates, _groupby_column_ids);
}

void AggregateHashSort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant> &parameters) {}

void AggregateHashSort::_on_cleanup() {}

std::shared_ptr<const Table> AggregateHashSort::_on_execute() {
  auto &input_table = *input_table_left();

  const auto fixed_size_groups = std::all_of(_groupby_column_ids.begin(), _groupby_column_ids.end(), [&](const ColumnID column_id) {
    return input_table.column_data_type(column_id) != DataType::String;
  });

  if (fixed_size_groups) {
    auto input_groups = produce_initial_group_run<FixedSizeGroupRun>(input_table, _groupby_column_ids);
    auto input_aggregates = produce_initial_aggregates(input_table, _aggregates);

    auto input_run = Run{input_table.row_count(), std::move(input_groups), std::move(input_aggregates)};

    // //std::cout << "Data: " << input_run.groups.data.size() << std::endl;

    std::vector<Run<FixedSizeGroupRun>> input_runs;
    input_runs.emplace_back(std::move(input_run));

    auto result_runs = aggregate<FixedSizeGroupRun>(std::move(input_runs), 1u);

    /**
     * Build output Table
     */
    const auto output_row_count = std::accumulate(result_runs.begin(), result_runs.end(), size_t{0}, [](const auto row_count, const auto& run) {
      return row_count + run.size;
    });

    const auto output_column_definitions = _get_output_column_defintions();

    auto output_segments = Segments{_aggregates.size() + _groupby_column_ids.size()};

    for (auto output_group_by_column_id = ColumnID{0}; output_group_by_column_id < _groupby_column_ids.size(); ++output_group_by_column_id) {
      const auto& output_column_definition = output_column_definitions[output_group_by_column_id];

      resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        auto values = std::vector<ColumnDataType>(output_row_count);
        auto null_values = std::vector<bool>();
        if (output_column_definition.nullable) {
          null_values.resize(output_row_count);
        }

        auto target_offset = size_t{0};
        for (auto&& run : result_runs) {
          run.groups.materialize_output<ColumnDataType>(values, null_values, target_offset, output_group_by_column_id);
          target_offset += run.size;
        }

        if (output_column_definition.nullable) {
          output_segments[output_group_by_column_id] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
        } else {
          output_segments[output_group_by_column_id] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
        }
      });
    }

//    for (auto aggregate_idx = size_t{0}; aggregate_idx < _aggregates.size(); ++aggregate_idx) {
//      const auto& output_column_definition = output_column_definitions[aggregate_idx + _groupby_column_ids.size()];
//
//      resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
//        using ColumnDataType = typename decltype(data_type_t)::type;
//
//        ColumnMaterialization<ColumnDataType> materialization;
//
//        materialization.values.resize(output_row_count);
//        if (output_column_definition.nullable) {
//          materialization.null_values.resize(output_row_count);
//        }
//
//        auto target_offset = size_t{0};
//        for (auto&& run : result_runs) {
//          //// //std::cout << "Materializing groupby run of size " << run.size << std::endl;
//          run.aggregates[aggregate_idx]->materialize(materialization, target_offset);
//          target_offset += run.size;
//        }
//
//        if (output_column_definition.nullable) {
//          output_segments[aggregate_idx + _groupby_column_ids.size()] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(materialization.values), std::move(materialization.null_values));
//        } else {
//          output_segments[aggregate_idx + _groupby_column_ids.size()] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(materialization.values));
//        }
//      });
//    }
//
    const auto output_table = std::make_shared<Table>(output_column_definitions, TableType::Data);
    output_table->append_chunk(output_segments);

    return output_table;
  } else {
    Fail("Nope");
  }
}


}  // namespace opossum
