#include "aggregate_hashsort.hpp"

#include "boost/functional/hash.hpp"

#include "storage/segment_iterate.hpp"
#include "operators/aggregate/aggregate_traits.hpp"

namespace {

using namespace opossum;  // NOLINT

struct BaseRunSegment {
  virtual ~BaseRunSegment() = default;

  virtual std::unique_ptr<BaseRunSegment> new_instance() const = 0;
};

struct FixedGroupByRunSegment {
  size_t group_size{};
  std::vector<char> data;

  auto group_value(const size_t offset) const {
    const auto begin = data.cbegin() + offset * group_size;
    return std::make_pair(begin, begin + group_size);
  }

  template<typename T>
  void materialize(std::vector<T>& values, std::vector<bool>& null_values, size_t source_offset, size_t target_offset) const {
    const auto nullable = !null_values.empty();
    const auto *source = reinterpret_cast<const char*>(&data[source_offset]);

    while (source_offset < data.size()) {
      if (nullable) {
        null_values[target_offset] = *source != 0;
        memcpy(reinterpret_cast<char*>(&values[target_offset]), source + 1, sizeof(T));
      } else {
        memcpy(reinterpret_cast<char*>(&values[target_offset]), source, sizeof(T));
      }

      source += group_size;
      source_offset += group_size;

      ++target_offset;
    }
  }

  template<>
  void materialize<pmr_string>(std::vector<pmr_string>& values, std::vector<bool>& null_values, size_t source_offset, size_t target_offset) const {
    std::cout << "Nooooooooooooooooo"  << std::endl;
  }
};

struct VariablySizedGroupByRunSegment {
  std::vector<size_t> group_offsets;
  std::vector<char> data;
};

template<typename SourceColumnDataType>
struct SumRunSegment : public BaseRunSegment {
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Sum>::AggregateType;

  void aggregate(const BaseRunSegment& base_source_segment, const std::vector<std::pair<size_t, size_t>>& aggregation_buffer) {
    const auto& source_segment = static_cast<const SumRunSegment<SourceColumnDataType>&>(base_source_segment);

    for (const auto& [source_offset, target_offset] : aggregation_buffer) {
      const auto& source_sum = source_segment.sums[source_offset];
      auto& target_sum = sums[target_offset];

      if (!source_sum) continue;

      if (!target_sum) {
        target_sum = source_sum;
      } else {
        *target_sum += *source_sum;
      }
    }
  }

  std::unique_ptr<BaseRunSegment> new_instance() const override {
    return std::make_unique<SumRunSegment>();
  }

  std::vector<std::optional<AggregateType>> sums;
};


template<typename GroupByRunSegment>
struct Run {
  bool is_aggregated{false};
  GroupByRunSegment group_by;
  std::vector<std::unique_ptr<BaseRunSegment>> aggregates;
  size_t size{0};

  size_t group_hash(const size_t offset) const {
    const auto group_value = group_by.group_value(offset);
    return boost::hash_range(group_value.first, group_value.second);
  }
};

template<typename GroupByRunSegment>
struct Partition {
  std::vector<std::pair<size_t, size_t>> aggregation_buffer;
  std::vector<size_t> partition_buffer;

  size_t group_key_counter{0};

  std::vector<Run<GroupByRunSegment>> runs;
};

template<typename GroupByRunSegment>
void append(GroupByRunSegment& target, const GroupByRunSegment& source, const size_t offset) {
  const auto source_value_range = source.group_value(offset);
  target.data.insert(target.data.end(), source_value_range.first, source_value_range.second);
}

template<typename GroupByRunSegment>
void copy(GroupByRunSegment& target, const GroupByRunSegment& source, const size_t target_offset, const size_t source_offset) {
  const auto source_value_range = source.group_value(source_offset);
  target.data.copy(target.data.begin() + target.group_by.group_size * target_offset, source_value_range.first, source_value_range.second);
}


template<typename GroupByRunSegment>
void flush_aggregation_buffer(Partition<GroupByRunSegment>& partition, const Run<GroupByRunSegment>& source_run) {
  if (partition.aggregation_buffer.empty()) return;

  const auto max_group_key = std::max_element(partition.aggregation_buffer.begin(), partition.aggregation_buffer.end(), [](const auto& entry) {
    return entry.second;
  });

  auto& target_run = partition.runs.back();

  if (max_group_key->second >= target_run.size) {
    target_run.group_by.resize(max_group_key->second + 1);
  }

  for (const auto& [source_offset, target_offset] : partition.aggregation_buffer) {
    copy(target_run.group_by, source_run.group_by, target_offset, source_offset);
  }

  partition.aggregation_buffer.clear();
}

template<typename GroupByRunSegment>
std::tuple<size_t, size_t, size_t> determine_partitioning(const std::vector<Run<GroupByRunSegment>>& runs, const size_t level) {
  return {2, level, 1};
}

template<typename GroupByRunSegment>
Run<GroupByRunSegment> make_run(const Run<GroupByRunSegment>& prototype);

template<>
Run<FixedGroupByRunSegment> make_run<FixedGroupByRunSegment>(const Run<FixedGroupByRunSegment>& prototype) {
  Run<FixedGroupByRunSegment> run;
  run.group_by = FixedGroupByRunSegment{prototype.group_by.group_size, {}};
  run.aggregates.resize(prototype.aggregates.size());
  for (auto aggregate_idx = size_t{0}; aggregate_idx < prototype.aggregates.size(); ++aggregate_idx) {
    run.aggregates[aggregate_idx] = prototype.aggregates[aggregate_idx]->new_instance();
  }

  return run;
}

template<typename GroupByRunSegment>
std::vector<Run<GroupByRunSegment>> aggregate(std::vector<Run<GroupByRunSegment>>&& runs, const size_t level) {
  if (runs.empty()) {
    return {};
  }

  if (runs.size() == 1 && runs.front().is_aggregated) {
    return std::move(runs);
  }

  const auto& [partition_count, partition_shift, partition_mask] = determine_partitioning(runs, level);

  auto partitions = std::vector<Partition<GroupByRunSegment>>{partition_count};
  for (auto& partition : partitions) {
    partition.runs.emplace_back(make_run(runs.front()));
  }

  const auto hash_fn = [&](const auto& key) {
    return runs[key.first].group_hash(key.second);
  };

  const auto compare_fn = [&](const auto& lhs, const auto& rhs) {
    const auto lhs_value_range = runs[lhs.first].group_by.group_value(lhs.second);
    const auto rhs_value_range = runs[lhs.first].group_by.group_value(lhs.second);
    return std::equal(lhs_value_range.first, lhs_value_range.second, rhs_value_range.first, rhs_value_range.second);
  };

  auto hash_table = std::unordered_map<std::pair<size_t, size_t>, size_t, decltype(hash_fn), decltype(compare_fn)>{0, hash_fn, compare_fn};

  for (auto run_idx = size_t{0}; run_idx < runs.size(); ++run_idx) {
    auto&& run = runs[run_idx];

    for (auto run_offset = size_t{0}; run_offset < run.size; ++run_offset) {
      auto partition_idx = run.group_hash(run_offset);
      partition_idx >>= partition_shift;
      partition_idx &= partition_mask;

      auto& partition = partitions[partition_idx];

      auto hash_table_iter = hash_table.find({run_idx, run_offset});
      if (hash_table_iter == hash_table.end()) {
        std::cout << run_idx << ", " << run_offset << " not found in hash table" << std::endl;
        hash_table_iter = hash_table.emplace(std::pair{run_idx, run_offset}, partition.group_key_counter).first;
        append(partition.runs.back().group_by, run.group_by, run_offset);
        ++partition.group_key_counter;
      } else {
        std::cout << run_idx << ", " << run_offset << " found in hash table" << std::endl;
      }

      std::cout << "  Partition key: " << hash_table_iter->second << std::endl;

      partition.aggregation_buffer.emplace_back(run_offset, hash_table_iter->second);

      if (partition.aggregation_buffer.size() > 255) {
        flush_aggregation_buffer(partition, run);
      }
    }

    for (auto& partition : partitions) {
      flush_aggregation_buffer(partition, run);
    }
  }

  for (auto& partition : partitions) {
    if (!partition.runs.empty() && partition.runs.back().size == 0) {
      partition.runs.pop_back();
    }
  }

  auto output_runs = std::vector<Run<GroupByRunSegment>>{};

  for (auto&& partition : partitions) {
    auto aggregated_partition = aggregate(std::move(partition.runs), level + 1);
    output_runs.insert(output_runs.end(), std::make_move_iterator(aggregated_partition.begin()), std::make_move_iterator(aggregated_partition.end()));
  }

  return output_runs;
}

std::vector<std::unique_ptr<BaseRunSegment>> initialize_aggregates(const Table& table, const std::vector<AggregateColumnDefinition>& aggregate_column_definitions) {
  auto aggregates = std::vector<std::unique_ptr<BaseRunSegment>>{aggregate_column_definitions.size()};
  for (auto aggregate_idx = size_t{0}; aggregate_idx < aggregates.size(); ++aggregate_idx) {
    const auto& aggregate_column_definition = aggregate_column_definitions[aggregate_idx];

    if (!aggregate_column_definition.column) {
      Fail("Nye");
    }

    resolve_data_type(table.column_data_type(*aggregate_column_definition.column), [&](const auto data_type_t) {
      using SourceColumnDataType = typename decltype(data_type_t)::type;

      switch (aggregate_column_definition.function) {
        case AggregateFunction::Min:
          aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>();
          break;
        case AggregateFunction::Max:
          aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>();
          break;
        case AggregateFunction::Sum:
          aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>();
          break;
        case AggregateFunction::Avg:
          aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>();
          break;
        case AggregateFunction::Count:
          aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>();
          break;
        case AggregateFunction::CountDistinct:
          aggregates[aggregate_idx] = std::make_unique<SumRunSegment<SourceColumnDataType>>();
          break;
      }
    });
  }

  return aggregates;
}

const auto data_type_size = std::unordered_map<DataType, size_t>{
  {DataType::Int, 4},
  {DataType::Long, 8},
  {DataType::Float, 4},
  {DataType::Double, 8}
};

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
    auto group_by_run_segment = FixedGroupByRunSegment{};

    auto group_value_sizes = std::vector<size_t>{_groupby_column_ids.size()};
    group_by_run_segment.group_size = {0};
    for (auto group_by_column_idx = size_t{0}; group_by_column_idx < _groupby_column_ids.size(); ++group_by_column_idx) {
      const auto group_by_column_id = _groupby_column_ids[group_by_column_idx];
      const auto group_value_size = data_type_size.at(input_table.column_data_type(group_by_column_id)) +
                                    (input_table.column_is_nullable(group_by_column_id) ? 1 : 0);
      group_value_sizes[group_by_column_idx] = group_value_size;
      group_by_run_segment.group_size += group_value_size;
    }

    group_by_run_segment.data.resize(input_table.row_count() * group_by_run_segment.group_size);

    auto chunk_begin_offset = size_t{0};
    for (const auto& chunk : input_table.chunks()) {
      auto segment_begin_offset = chunk_begin_offset;
      for (auto group_by_column_idx = size_t{0}; group_by_column_idx < _groupby_column_ids.size(); ++group_by_column_idx) {
        const auto group_value_size = group_value_sizes[group_by_column_idx];

        auto offset = segment_begin_offset;

        segment_iterate(*chunk->get_segment(_groupby_column_ids[group_by_column_idx]), [&](const auto& segment_position) {
          constexpr auto NULLABLE = std::decay_t<decltype(segment_position)>::Nullable;

          if (segment_position.is_null()) {
            group_by_run_segment.data[offset] = 1;
            memset(&group_by_run_segment.data[offset + 1], 0, group_value_size - 1);
          } else {
            if (NULLABLE) {
              group_by_run_segment.data[offset] = 0;
              memcpy(&group_by_run_segment.data[offset + 1], &segment_position.value(), group_value_size - 1);
            } else {
              memcpy(&group_by_run_segment.data[offset], &segment_position.value(), group_value_size);
            }
          }

          offset += group_by_run_segment.group_size;
        });

        segment_begin_offset += group_value_sizes[group_by_column_idx];
      }

      chunk_begin_offset += group_by_run_segment.group_size * chunk->size();
    }

    std::cout << "Group size: " << group_by_run_segment.group_size << std::endl;
    std::cout << "GroupByColumn size : " << group_by_run_segment.data.size() << std::endl;

    std::vector<Run<FixedGroupByRunSegment>> root_runs{1};
    root_runs.front().size = input_table.row_count();
    root_runs.front().group_by = std::move(group_by_run_segment);
    root_runs.front().aggregates = initialize_aggregates(input_table, _aggregates);

    const auto result_runs = aggregate<FixedGroupByRunSegment>(std::move(root_runs), 0u);

    /**
     * Build output Table
     */
    const auto output_row_count = std::accumulate(result_runs.begin(), result_runs.end(), size_t{0}, [](const auto row_count, const auto& run) {
      return row_count + run.size;
    });

    const auto output_column_definitions = _get_output_column_defintions();
    auto group_value_base_offset = size_t{0};

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
          std::cout << "Materializing groupby run of size " << run.size << std::endl;
          run.group_by.materialize<ColumnDataType>(values, null_values, group_value_base_offset, target_offset);
          target_offset += run.size;
        }

        if (output_column_definition.nullable) {
          output_segments[output_group_by_column_id] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
        } else {
          output_segments[output_group_by_column_id] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
        }
      });

      group_value_base_offset += group_value_sizes[output_group_by_column_id];
    }

    const auto output_table = std::make_shared<Table>(output_column_definitions, TableType::Data);
    output_table->append_chunk(std::move(output_segments));

    return output_table;
  } else {
    Fail("Nope");
  }
}


}  // namespace opossum
