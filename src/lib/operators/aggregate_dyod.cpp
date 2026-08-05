#include "aggregate_dyod.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <functional>
#include <limits>
#include <memory>
#include <memory_resource>
#include <numeric>
#include <span>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/distinct_set.hpp"
#include "operators/aggregate_dyod/hyperloglog.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "operators/aggregate_dyod/merge_map.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "operators/aggregate_dyod/scatter_store.hpp"
#include "operators/operator_performance_data.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/attribute_statistics.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace hyrise {

class AbstractAggregator {
 public:
  virtual ~AbstractAggregator() = default;
  virtual void set_worker_count(size_t worker_count) = 0;
  virtual void accumulate(size_t worker_id, const Chunk& chunk) = 0;
  virtual void merge() = 0;
  virtual std::shared_ptr<AbstractSegment> build_segment() const = 0;
  virtual TableColumnDefinition output_column_definition() const = 0;
};

namespace {
template <typename ColumnDataType, WindowFunction window_function>
class StandardAggregator : public AbstractAggregator {
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, window_function>::ReturnType;

  struct State {
    AggregateType accumulator{};
    size_t count{0};
  };

  struct alignas(64) PaddedState {
    State state{};
  };

 public:
  StandardAggregator(std::string output_name, const ColumnID column_id)
      : _output_name{std::move(output_name)}, _column_id{column_id} {}

  void set_worker_count(const size_t worker_count) override {
    _states.assign(worker_count, PaddedState{});
  }

  void accumulate(const size_t worker_id, const Chunk& chunk) override {
    auto& state = _states[worker_id].state;
    const auto fold = WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>{}.get_aggregate_function();

    if constexpr (window_function == WindowFunction::Min || window_function == WindowFunction::Max) {
      // Pruning statistics store exact chunk extrema, so MIN/MAX usually need no segment scan.
      const auto pruning_statistics = chunk.pruning_statistics();
      if (pruning_statistics && static_cast<size_t>(_column_id) < pruning_statistics->size()) {
        const auto& attribute_statistics =
            static_cast<const AttributeStatistics<ColumnDataType>&>(*(*pruning_statistics)[_column_id]);
        // For MIN/MAX, count only records whether a value was seen; one exact extremum represents the whole chunk.
        if (attribute_statistics.min_max_filter) {
          const auto& filter = *attribute_statistics.min_max_filter;
          const auto& value = window_function == WindowFunction::Min ? filter.min : filter.max;
          fold(value, state.count, state.accumulator);
          ++state.count;
          return;
        }

        if constexpr (std::is_arithmetic_v<ColumnDataType>) {
          if (attribute_statistics.range_filter) {
            const auto& ranges = attribute_statistics.range_filter->ranges;
            Assert(!ranges.empty(), "A RangeFilter used for MIN/MAX must contain at least one range.");
            const auto& value = window_function == WindowFunction::Min ? ranges.front().first : ranges.back().second;
            fold(value, state.count, state.accumulator);
            ++state.count;
            return;
          }
        }

        if (attribute_statistics.distinct_value_count && attribute_statistics.distinct_value_count->count == 0) {
          return;
        }
      }
    }

    const auto& segment = *chunk.get_segment(_column_id);
    if constexpr (window_function == WindowFunction::Min || window_function == WindowFunction::Max) {
      if (const auto* dictionary = dynamic_cast<const BaseDictionarySegment*>(&segment)) {
        const auto distinct_count = dictionary->unique_values_count();

        if (distinct_count == 0) {
          return;
        }

        const auto candidate_id = (window_function == WindowFunction::Min) ? ValueID{0} : ValueID{distinct_count - 1};
        const auto candidate = boost::get<ColumnDataType>(dictionary->value_of_value_id(candidate_id));

        fold(candidate, state.count, state.accumulator);
        ++state.count;
        return;
      }
    }

    segment_iterate<ColumnDataType>(segment, [&](const auto& position) {
      if (position.is_null()) {
        return;
      }
      fold(position.value(), state.count, state.accumulator);
      ++state.count;
    });
  }

  void merge() override {
    for (const auto& padded : _states) {
      const auto& state = padded.state;
      if (state.count == 0) {
        continue;
      }

      if constexpr (window_function == WindowFunction::Min) {
        if (_final.count == 0 || value_smaller(state.accumulator, _final.accumulator)) {
          _final.accumulator = state.accumulator;
        }
      } else if constexpr (window_function == WindowFunction::Max) {
        if (_final.count == 0 || value_greater(state.accumulator, _final.accumulator)) {
          _final.accumulator = state.accumulator;
        }
      } else {
        _final.accumulator += state.accumulator;
      }

      _final.count += state.count;
    }
  }

  std::shared_ptr<AbstractSegment> build_segment() const override {
    auto values = pmr_vector<AggregateType>{};
    auto null_values = pmr_vector<bool>{};

    if (_final.count == 0) {
      values.emplace_back();
      null_values.emplace_back(true);
    } else {
      if constexpr (window_function == WindowFunction::Avg) {
        if constexpr (std::is_arithmetic_v<ColumnDataType>) {
          values.emplace_back(_final.accumulator / static_cast<AggregateType>(_final.count));
        } else {
          Fail("AVG is only defined on arithmetic columns.");
        }
      } else {
        values.emplace_back(_final.accumulator);
      }
      null_values.emplace_back(false);
    }

    return std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(null_values));
  }

  TableColumnDefinition output_column_definition() const override {
    return TableColumnDefinition{_output_name, data_type_from_type<AggregateType>(), true};
  }

 private:
  std::string _output_name;
  ColumnID _column_id;
  std::vector<PaddedState> _states;
  State _final{};
};

// ANY keeps the carried input column's name and nullability, like AggregateHash.
template <typename ColumnDataType>
class AnyAggregator : public AbstractAggregator {
  struct State {
    AllTypeVariant value;
    bool seen{false};
  };

  struct alignas(64) PaddedState {
    State state{};
  };

 public:
  AnyAggregator(std::string output_name, const ColumnID column_id, const bool nullable)
      : _output_name{std::move(output_name)}, _column_id{column_id}, _nullable{nullable} {}

  void set_worker_count(const size_t worker_count) override {
    _states.assign(worker_count, PaddedState{});
  }

  void accumulate(const size_t worker_id, const Chunk& chunk) override {
    auto& state = _states[worker_id].state;
    if (state.seen || chunk.size() == 0) {
      return;
    }
    state.value = (*chunk.get_segment(_column_id))[ChunkOffset{0}];
    state.seen = true;
  }

  void merge() override {
    for (const auto& padded : _states) {
      if (padded.state.seen) {
        _final = padded.state;
        break;
      }
    }
  }

  std::shared_ptr<AbstractSegment> build_segment() const override {
    const auto is_null = !_final.seen || variant_is_null(_final.value);
    auto values = pmr_vector<ColumnDataType>{};
    values.emplace_back(is_null ? ColumnDataType{} : boost::get<ColumnDataType>(_final.value));
    if (!_nullable) {
      return std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
    }

    return std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), pmr_vector<bool>{is_null});
  }

  TableColumnDefinition output_column_definition() const override {
    return TableColumnDefinition{_output_name, data_type_from_type<ColumnDataType>(), _nullable};
  }

 private:
  std::string _output_name;
  ColumnID _column_id;
  bool _nullable;
  std::vector<PaddedState> _states;
  State _final{};
};

class CountStarAggregator : public AbstractAggregator {
  struct alignas(64) PaddedCount {
    size_t count{0};
  };

 public:
  explicit CountStarAggregator(std::string output_name) : _output_name{std::move(output_name)} {}

  void set_worker_count(const size_t worker_count) override {
    _states.assign(worker_count, PaddedCount{});
  }

  void accumulate(const size_t worker_id, const Chunk& chunk) override {
    _states[worker_id].count += chunk.size();
  }

  void merge() override {
    for (const auto& [count] : _states) {
      _final += count;
    }
  }

  std::shared_ptr<AbstractSegment> build_segment() const override {
    return std::make_shared<ValueSegment<int64_t>>(pmr_vector{static_cast<int64_t>(_final)});
  }

  TableColumnDefinition output_column_definition() const override {
    return TableColumnDefinition{_output_name, DataType::Long, false};
  }

 private:
  std::string _output_name;
  std::vector<PaddedCount> _states;
  size_t _final{0};
};

template <typename ColumnDataType>
class CountColumnAggregator : public AbstractAggregator {
  struct alignas(64) PaddedCount {
    size_t count{0};
  };

 public:
  CountColumnAggregator(std::string output_name, const ColumnID column_id)
      : _output_name{std::move(output_name)}, _column_id{column_id} {}

  void set_worker_count(const size_t worker_count) override {
    _states.assign(worker_count, PaddedCount{});
  }

  void accumulate(const size_t worker_id, const Chunk& chunk) override {
    auto& count = _states[worker_id].count;
    segment_iterate<ColumnDataType>(*chunk.get_segment(_column_id), [&](const auto& position) {
      if (!position.is_null()) {
        ++count;
      }
    });
  }

  void merge() override {
    for (const auto& [count] : _states) {
      _final += count;
    }
  }

  std::shared_ptr<AbstractSegment> build_segment() const override {
    return std::make_shared<ValueSegment<int64_t>>(pmr_vector{static_cast<int64_t>(_final)});
  }

  TableColumnDefinition output_column_definition() const override {
    return TableColumnDefinition{_output_name, DataType::Long, false};
  }

 private:
  std::string _output_name;
  ColumnID _column_id;
  std::vector<PaddedCount> _states;
  size_t _final{0};
};

template <typename ColumnDataType>
class CountDistinctAggregator : public AbstractAggregator {
  struct alignas(64) PaddedState {
    DistinctSet<ColumnDataType> set;
  };

 public:
  CountDistinctAggregator(std::string output_name, const ColumnID column_id)
      : _output_name{std::move(output_name)}, _column_id{column_id} {}

  void set_worker_count(const size_t worker_count) override {
    _states = std::vector<PaddedState>(worker_count);
  }

  void accumulate(const size_t worker_id, const Chunk& chunk) override {
    auto& set = _states[worker_id].set;
    segment_iterate<ColumnDataType>(*chunk.get_segment(_column_id), [&](const auto& position) {
      if (position.is_null()) {
        return;
      }
      set.insert(0, position.value());
    });
  }

  void merge() override {
    for (auto worker_id = size_t{1}; worker_id < _states.size(); ++worker_id) {
      _states.front().set.merge(_states[worker_id].set);
    }
  }

  std::shared_ptr<AbstractSegment> build_segment() const override {
    return std::make_shared<ValueSegment<int64_t>>(pmr_vector{static_cast<int64_t>(_states.front().set.size())});
  }

  TableColumnDefinition output_column_definition() const override {
    return TableColumnDefinition{_output_name, DataType::Long, false};
  }

 private:
  std::string _output_name;
  ColumnID _column_id;
  std::vector<PaddedState> _states;
};

std::unique_ptr<AbstractAggregator> make_aggregator(const Table& input_table, const WindowFunctionExpression& aggregate,
                                                    const ColumnID column_id) {
  const auto window_function = aggregate.window_function;
  auto output_name = aggregate.as_column_name();

  if (window_function == WindowFunction::Count && column_id == INVALID_COLUMN_ID) {
    return std::make_unique<CountStarAggregator>(std::move(output_name));
  }
  Assert(column_id != INVALID_COLUMN_ID, "Only COUNT(*) can have an invalid ColumnID.");
  if (window_function == WindowFunction::Any) {
    output_name = input_table.column_name(column_id);
  }

  auto aggregator = std::unique_ptr<AbstractAggregator>{};
  resolve_data_type(input_table.column_data_type(column_id), [&](const auto type) {
    using ColumnDataType = typename decltype(type)::type;
    switch (window_function) {
      case WindowFunction::Sum:
        aggregator = std::make_unique<StandardAggregator<ColumnDataType, WindowFunction::Sum>>(std::move(output_name),
                                                                                               column_id);
        break;
      case WindowFunction::Min:
        aggregator = std::make_unique<StandardAggregator<ColumnDataType, WindowFunction::Min>>(std::move(output_name),
                                                                                               column_id);
        break;
      case WindowFunction::Max:
        aggregator = std::make_unique<StandardAggregator<ColumnDataType, WindowFunction::Max>>(std::move(output_name),
                                                                                               column_id);
        break;
      case WindowFunction::Avg:
        aggregator = std::make_unique<StandardAggregator<ColumnDataType, WindowFunction::Avg>>(std::move(output_name),
                                                                                               column_id);
        break;
      case WindowFunction::Count:
        aggregator = std::make_unique<CountColumnAggregator<ColumnDataType>>(std::move(output_name), column_id);
        break;
      case WindowFunction::CountDistinct:
        aggregator = std::make_unique<CountDistinctAggregator<ColumnDataType>>(std::move(output_name), column_id);
        break;
      case WindowFunction::Any:
        aggregator = std::make_unique<AnyAggregator<ColumnDataType>>(std::move(output_name), column_id,
                                                                     input_table.column_is_nullable(column_id));
        break;
      default:
        Fail("WindowFunction not yet supported");
    }
  });

  return aggregator;
}

std::vector<std::unique_ptr<AbstractAggregator>> build_aggregators(
    const Table& input_table, const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates) {
  auto aggregators = std::vector<std::unique_ptr<AbstractAggregator>>{};
  aggregators.reserve(aggregates.size());

  for (const auto& aggregate : aggregates) {
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    aggregators.push_back(make_aggregator(input_table, *aggregate, pqp_column.column_id));
  }

  return aggregators;
}

template <typename Worker>
void run_workers(const size_t worker_count, const Worker& worker) {
  // The immediate scheduler executes JobTasks sequentially; direct calls avoid their setup overhead.
  if (!Hyrise::get().is_multi_threaded()) {
    for (auto worker_id = size_t{0}; worker_id < worker_count; ++worker_id) {
      worker(worker_id);
    }
    return;
  }

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(worker_count);
  for (auto worker_id = size_t{0}; worker_id < worker_count; ++worker_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&worker, worker_id]() {
      worker(worker_id);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

bool low_cardinality_eligible(const AggregateSchema& schema, const Table& input) {
  for (auto i = size_t{0}; i < schema.aggregate_count(); ++i) {
    if (const auto fn = schema.function(i); fn == WindowFunction::Any || fn == WindowFunction::CountDistinct)
      return false;
    if (const auto col = schema.source_column(i);
        col != INVALID_COLUMN_ID && input.column_data_type(col) == DataType::String)
      return false;
  }
  return true;
}

void gather_value_column(const AbstractSegment& segment, const DataType type, const bool nullable,
                         std::vector<std::byte>& out_bytes, std::vector<std::byte>& out_null, const size_t row_count) {
  resolve_data_type(type, [&](const auto data_type) {
    using ColumnDataType = typename decltype(data_type)::type;
    out_bytes.assign(row_count * sizeof(ColumnDataType), std::byte{0});
    if (nullable) {
      out_null.assign((row_count + 7) / 8, std::byte{0});
    }
    auto row = size_t{0};
    segment_iterate<ColumnDataType>(segment, [&](const auto& position) {
      if (position.is_null()) {
        if (nullable) {
          out_null[row / 8] |= std::byte{1} << (row % 8);
        }
      } else {
        const auto value = position.value();
        std::memcpy(out_bytes.data() + row * sizeof(ColumnDataType), &value, sizeof(ColumnDataType));
      }
      ++row;
    });
  });
}
}  // namespace

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids,
                                std::make_unique<OperatorPerformanceData<OperatorSteps>>()) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  const auto input_table = left_input_table();
  const auto aggregate_schema = _prepare(*input_table);

  if (_groupby_column_ids.empty()) {
    return _aggregate_without_group_by(aggregate_schema, *input_table);
  }

  auto output_table = std::shared_ptr<Table>{};
  resolve_key_schema(_groupby_column_ids, *input_table, [&](const auto& key_schema) {
    output_table = _aggregate(key_schema, aggregate_schema, *input_table);
  });
  return output_table;
}

AggregateSchema AggregateDYOD::_prepare(const Table& input_table) {
  _validate_aggregates();
  for (const auto& aggregate : _aggregates) {
    const auto function = aggregate->window_function;
    Assert(function == WindowFunction::Sum || function == WindowFunction::Min || function == WindowFunction::Max ||
               function == WindowFunction::Avg || function == WindowFunction::Count ||
               function == WindowFunction::CountDistinct || function == WindowFunction::Any,
           "WindowFunction not yet supported");
  }
  return AggregateSchema::build(_aggregates, input_table);
}

template <typename KeySchema>
std::shared_ptr<Table> AggregateDYOD::_aggregate_low_cardinality(const KeySchema& key_schema,
                                                                 const AggregateSchema& aggregate_schema,
                                                                 const Table& input_table,
                                                                 const size_t cardinality_estimate) {
  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  auto timer = Timer{};

  const auto chunk_count = static_cast<size_t>(input_table.chunk_count());
  const auto num_cpus = std::max(size_t{1}, Hyrise::get().topology.num_cpus());
  const auto worker_count = std::clamp(chunk_count, size_t{1}, num_cpus);
  const auto aggregate_count = aggregate_schema.aggregate_count();
  const auto key_width = key_schema.packed_width();

  auto per_worker_private_maps = std::vector<MergeMap<KeySchema>>{};
  per_worker_private_maps.reserve(worker_count);
  for (auto worker = size_t{0}; worker < worker_count; worker++) {
    per_worker_private_maps.emplace_back(key_schema, uint32_t{0}, aggregate_schema.make_accumulator_columns());
  }

  {
    auto chunk_cursor = std::atomic<size_t>{0};
    run_workers(worker_count, [&](const size_t worker_id) {
      auto& merge_map = per_worker_private_maps[worker_id];
      merge_map.reserve(cardinality_estimate);

      auto decode_scratch = KeyDecodeScratch{};
      auto spill_scratch = StringSpillBuffer{};
      auto key_buffer = std::vector<std::byte>{};
      auto slots = std::vector<uint32_t>{};
      auto value_buffers = std::vector<std::vector<std::byte>>(aggregate_count);
      auto null_buffers = std::vector<std::vector<std::byte>>(aggregate_count);
      auto owners = std::vector<std::shared_ptr<AbstractSegment>>{};
      auto segments = std::vector<const AbstractSegment*>{};

      while (true) {
        const auto chunk_index = chunk_cursor.fetch_add(1, std::memory_order_relaxed);
        if (chunk_index >= chunk_count) {
          break;
        }
        const auto chunk = input_table.get_chunk(ChunkID{static_cast<ChunkID::base_type>(chunk_index)});
        if (!chunk) {
          continue;
        }

        const auto row_count = chunk->size();
        if (row_count == 0) {
          continue;
        }

        owners.clear();
        segments.clear();
        for (const auto column_id : _groupby_column_ids) {
          owners.emplace_back(chunk->get_segment(column_id));
          segments.emplace_back(owners.back().get());
        }
        key_schema.decode(segments, decode_scratch);
        key_buffer.resize(size_t{row_count} * key_width);
        for (auto offset = ChunkOffset{0}; offset < row_count; ++offset) {
          key_schema.pack(decode_scratch, offset, key_buffer.data() + size_t{offset} * key_width, spill_scratch);
        }

        for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
          const auto stream = aggregate_schema.aggregate_value_stream(aggregate_index);
          if (stream == AggregateSchema::NO_VALUE_STREAM) {
            continue;
          }
          const auto source = aggregate_schema.source_column(aggregate_index);
          const auto nullable = aggregate_schema.value_stream(stream).is_nullable();
          gather_value_column(*chunk->get_segment(source), input_table.column_data_type(source), nullable,
                              value_buffers[aggregate_index], null_buffers[aggregate_index], row_count);
        }

        for (auto tile_start = size_t{0}; tile_start < row_count; tile_start += MERGE_TILE_ROWS) {
          const auto tile_rows = std::min(MERGE_TILE_ROWS, size_t{row_count} - tile_start);
          slots.clear();
          merge_map.resolve({key_buffer.data() + tile_start * key_width, tile_rows * key_width}, slots);

          for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
            const auto stream = aggregate_schema.aggregate_value_stream(aggregate_index);
            if (stream == AggregateSchema::NO_VALUE_STREAM) {
              merge_map.fold(aggregate_index, slots, {}, {});
              continue;
            }
            const auto width = aggregate_schema.value_stream(stream).element_width();
            const auto nullable = aggregate_schema.value_stream(stream).is_nullable();
            const auto value_span = std::span<const std::byte>{
                value_buffers[aggregate_index].data() + tile_start * width, tile_rows * width};
            auto null_span = std::span<const std::byte>{};
            if (nullable) {
              null_span = {null_buffers[aggregate_index].data() + tile_start / 8, (tile_rows + 7) / 8};
            }
            merge_map.fold(aggregate_index, slots, value_span, null_span);
          }
        }
        spill_scratch.clear();
      }
    });
  }
  step_performance_data.set_step_runtime(OperatorSteps::Scatter, timer.lap());

  // reduce the per-worker private maps into map 0, then emit its groups as the result.
  {
    auto& combined = per_worker_private_maps.front();
    for (auto worker = size_t{1}; worker < worker_count; ++worker) {
      combined.combine(per_worker_private_maps[worker]);
    }
  }

  auto output_column_definitions = TableColumnDefinitions{};
  output_column_definitions.reserve(_groupby_column_ids.size() + aggregate_count);
  for (const auto column_id : _groupby_column_ids) {
    output_column_definitions.emplace_back(input_table.column_name(column_id), input_table.column_data_type(column_id),
                                           input_table.column_is_nullable(column_id));
  }
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto function = aggregate_schema.function(aggregate_index);
    output_column_definitions.emplace_back(
        _aggregates[aggregate_index]->as_column_name(), aggregate_schema.result_type(aggregate_index),
        function != WindowFunction::Count && function != WindowFunction::CountDistinct);
  }

  auto per_worker_outputs = std::vector<OutputColumns>{};
  per_worker_outputs.emplace_back(output_column_definitions, Chunk::DEFAULT_SIZE);
  {
    per_worker_private_maps.front().flush_into(per_worker_outputs.front());
    per_worker_outputs.front().seal_all();
  }
  auto output_table = build_output_table(output_column_definitions, per_worker_outputs);
  step_performance_data.set_step_runtime(OperatorSteps::Merge, timer.lap());
  return output_table;
}

template <typename KeySchema>
std::shared_ptr<Table> AggregateDYOD::_aggregate(const KeySchema& key_schema, const AggregateSchema& aggregate_schema,
                                                 const Table& input_table) {
  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  auto timer = Timer{};

  const auto chunk_count = static_cast<size_t>(input_table.chunk_count());
  const auto num_cpus = std::max(size_t{1}, Hyrise::get().topology.num_cpus());
  const auto scatter_worker_count = std::clamp(chunk_count, size_t{1}, num_cpus);
  const auto key_width = key_schema.packed_width();

  const auto gather_group_by_segments = [&](const Chunk& chunk, std::vector<std::shared_ptr<AbstractSegment>>& owners,
                                            std::vector<const AbstractSegment*>& segments) {
    owners.clear();
    segments.clear();
    for (const auto column_id : _groupby_column_ids) {
      owners.emplace_back(chunk.get_segment(column_id));
      segments.emplace_back(owners.back().get());
    }
  };

  // Estimate: per-worker HyperLogLog sketches over the packed-key hash choose the partition count.
  const auto estimate_worker_count =
      input_table.row_count() < PARALLEL_ESTIMATE_THRESHOLD ? size_t{1} : scatter_worker_count;
  auto sketches = std::vector<HllSketch>(estimate_worker_count);
  {
    auto chunk_cursor = std::atomic<size_t>{0};
    run_workers(estimate_worker_count, [&](const size_t worker_id) {
      auto& sketch = sketches[worker_id];
      auto key_scratch = std::vector<std::byte>(key_width);
      auto decode_scratch = KeyDecodeScratch{};
      auto spill_scratch = StringSpillBuffer{};
      auto segment_owners = std::vector<std::shared_ptr<AbstractSegment>>{};
      auto segments = std::vector<const AbstractSegment*>{};
      while (true) {
        const auto chunk_index = chunk_cursor.fetch_add(1, std::memory_order_relaxed);
        if (chunk_index >= chunk_count) {
          break;
        }
        const auto chunk = input_table.get_chunk(ChunkID{static_cast<ChunkID::base_type>(chunk_index)});
        if (!chunk) {
          continue;
        }
        gather_group_by_segments(*chunk, segment_owners, segments);
        key_schema.decode(segments, decode_scratch);
        const auto row_count = chunk->size();
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < row_count; ++chunk_offset) {
          key_schema.pack(decode_scratch, chunk_offset, key_scratch.data(), spill_scratch);
          sketch.add(key_schema.hash(key_scratch.data()));
        }
        spill_scratch.clear();
      }
    });
  }
  for (auto worker_id = size_t{1}; worker_id < estimate_worker_count; ++worker_id) {
    sketches.front().merge(sketches[worker_id]);
  }
  const auto cardinality_estimate = sketches.front().estimate();

  step_performance_data.set_step_runtime(OperatorSteps::Estimate, timer.lap());

  if (cardinality_estimate <= LOW_CARDINALITY_THRESHOLD && low_cardinality_eligible(aggregate_schema, input_table)) {
    return _aggregate_low_cardinality(key_schema, aggregate_schema, input_table, cardinality_estimate);
  }

  const auto partition_count = choose_partition_count(cardinality_estimate, num_cpus);
  const auto shift = static_cast<uint32_t>(std::countr_zero(partition_count));

  const auto value_stream_count = aggregate_schema.value_stream_count();
  auto value_stream_widths = std::vector<size_t>(value_stream_count);
  auto value_stream_sources = std::vector<ColumnID>(value_stream_count, INVALID_COLUMN_ID);
  auto value_stream_null_bits = std::vector<uint32_t>(value_stream_count, 0);
  auto nullable_stream_count = uint32_t{0};
  for (auto stream_index = size_t{0}; stream_index < value_stream_count; ++stream_index) {
    const auto& stream = aggregate_schema.value_stream(stream_index);
    value_stream_widths[stream_index] = stream.element_width();
    if (stream.is_nullable()) {
      value_stream_null_bits[stream_index] = nullable_stream_count;
      ++nullable_stream_count;
    }
  }
  const auto aggregate_count = aggregate_schema.aggregate_count();
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto stream_index = aggregate_schema.aggregate_value_stream(aggregate_index);
    if (stream_index == AggregateSchema::NO_VALUE_STREAM) {
      continue;
    }
    value_stream_sources[stream_index] = aggregate_schema.source_column(aggregate_index);
  }

  const auto value_null_bitmap_width = aggregate_schema.value_null_bitmap_width();
  const auto has_value_null_bitmap = value_null_bitmap_width > 0;
  const auto needs_value_arena = aggregate_schema.needs_value_arena();

  // ANY aggregates scatter no value; they read one shared row-id stream instead.
  const auto needs_row_id_stream = aggregate_schema.needs_row_id_stream();
  const auto row_id_stream_index = value_stream_count;
  if (needs_row_id_stream) {
    value_stream_widths.emplace_back(sizeof(RowID));
  }
  const auto value_null_bitmap_stream_index = 1 + value_stream_widths.size();

  // The packed key is staged in 4-byte pieces and the value-null bitmap byte-wise, so every declared stream width
  // divides the SWWC line.
  auto stream_widths = std::vector<size_t>{};
  stream_widths.reserve(1 + value_stream_widths.size() + (has_value_null_bitmap ? 1 : 0));
  stream_widths.emplace_back(4);
  stream_widths.insert(stream_widths.end(), value_stream_widths.begin(), value_stream_widths.end());
  if (has_value_null_bitmap) {
    stream_widths.emplace_back(1);
  }

  auto scatter_stores = std::vector<ScatterStore>{};
  scatter_stores.reserve(scatter_worker_count);
  for (auto worker_id = size_t{0}; worker_id < scatter_worker_count; ++worker_id) {
    scatter_stores.emplace_back(partition_count, key_width, value_stream_widths, value_null_bitmap_width,
                                needs_value_arena);
  }

  // Scatter: buffer raw (key, values...) rows into per-worker stores across the partitions. The key pass packs and
  // routes each row and records its partition; the value streams, the row-id stream, and the value-null bitmap then
  // run as separate column-wise passes over the same chunk.
  {
    auto chunk_cursor = std::atomic<size_t>{0};
    run_workers(scatter_worker_count, [&](const size_t worker_id) {
      auto& store = scatter_stores[worker_id];
      auto heads = ScatterHeads{partition_count, stream_widths.size(), stream_widths, has_value_null_bitmap};
      auto key_scratch = std::vector<std::byte>(key_width);
      auto decode_scratch = KeyDecodeScratch{};
      auto row_partitions = std::vector<PartitionId>{};
      auto bitmap_scratch = std::vector<std::byte>{};
      auto pack_spill = StringSpillBuffer{};
      auto segment_owners = std::vector<std::shared_ptr<AbstractSegment>>{};
      auto segments = std::vector<const AbstractSegment*>{};
      auto value_segment_owners = std::vector<std::shared_ptr<AbstractSegment>>{};
      auto value_segments = std::vector<const AbstractSegment*>{};
      while (true) {
        const auto chunk_index = chunk_cursor.fetch_add(1, std::memory_order_relaxed);
        if (chunk_index >= chunk_count) {
          break;
        }
        const auto chunk = input_table.get_chunk(ChunkID{static_cast<ChunkID::base_type>(chunk_index)});
        if (!chunk) {
          continue;
        }
        gather_group_by_segments(*chunk, segment_owners, segments);
        value_segment_owners.clear();
        value_segments.clear();
        for (const auto column_id : value_stream_sources) {
          value_segment_owners.emplace_back(chunk->get_segment(column_id));
          value_segments.emplace_back(value_segment_owners.back().get());
        }
        const auto row_count = chunk->size();

        key_schema.decode(segments, decode_scratch);
        row_partitions.resize(row_count);
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < row_count; ++chunk_offset) {
          key_schema.pack(decode_scratch, chunk_offset, key_scratch.data(), pack_spill);
          const auto key_hash = key_schema.hash(key_scratch.data());
          const auto partition = static_cast<PartitionId>(key_hash & (partition_count - 1));
          row_partitions[chunk_offset] = partition;
          if constexpr (KeySchema::HAS_STRINGS) {
            key_schema.reintern_spill(key_scratch.data(), store.key_spill_buffer(partition));
            pack_spill.clear();
          }
          for (auto piece_offset = size_t{0}; piece_offset < key_width; piece_offset += 4) {
            heads.push(store, 0, partition, key_scratch.data() + piece_offset, 4);
          }
        }

        if (has_value_null_bitmap) {
          bitmap_scratch.assign(row_count * value_null_bitmap_width, std::byte{0});
        }
        for (auto stream_index = size_t{0}; stream_index < value_stream_count; ++stream_index) {
          aggregate_schema.value_stream(stream_index)
              .scatter(*value_segments[stream_index], row_partitions, 1 + stream_index, heads, store,
                       bitmap_scratch.data(), value_null_bitmap_width, value_stream_null_bits[stream_index]);
        }
        if (needs_row_id_stream) {
          for (auto chunk_offset = ChunkOffset{0}; chunk_offset < row_count; ++chunk_offset) {
            const auto row_id = RowID{ChunkID{static_cast<ChunkID::base_type>(chunk_index)}, chunk_offset};
            heads.push(store, 1 + row_id_stream_index, row_partitions[chunk_offset],
                       reinterpret_cast<const std::byte*>(&row_id), sizeof(row_id));
          }
        }
        if (has_value_null_bitmap) {
          for (auto chunk_offset = ChunkOffset{0}; chunk_offset < row_count; ++chunk_offset) {
            const auto* row_bitmap = bitmap_scratch.data() + size_t{chunk_offset} * value_null_bitmap_width;
            for (auto byte_index = size_t{0}; byte_index < value_null_bitmap_width; ++byte_index) {
              heads.push(store, value_null_bitmap_stream_index, row_partitions[chunk_offset], row_bitmap + byte_index,
                         1);
            }
          }
        }
      }
      heads.finish(store);
    });
  }
  step_performance_data.set_step_runtime(OperatorSteps::Scatter, timer.lap());

  auto output_column_definitions = TableColumnDefinitions{};
  output_column_definitions.reserve(_groupby_column_ids.size() + aggregate_count);
  for (const auto column_id : _groupby_column_ids) {
    output_column_definitions.emplace_back(input_table.column_name(column_id), input_table.column_data_type(column_id),
                                           input_table.column_is_nullable(column_id));
  }
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto function = aggregate_schema.function(aggregate_index);
    // ANY keeps the input column's name and nullability.
    if (function == WindowFunction::Any) {
      const auto source_column = aggregate_schema.source_column(aggregate_index);
      output_column_definitions.emplace_back(input_table.column_name(source_column),
                                             aggregate_schema.result_type(aggregate_index),
                                             input_table.column_is_nullable(source_column));
      continue;
    }
    output_column_definitions.emplace_back(
        _aggregates[aggregate_index]->as_column_name(), aggregate_schema.result_type(aggregate_index),
        function != WindowFunction::Count && function != WindowFunction::CountDistinct);
  }

  // Merge: workers claim partitions and fold every store's rows for that partition through a dense MergeMap.
  const auto merge_worker_count = std::min(static_cast<size_t>(partition_count), num_cpus);
  auto per_worker_outputs = std::vector<OutputColumns>{};
  per_worker_outputs.reserve(merge_worker_count);
  for (auto worker_id = size_t{0}; worker_id < merge_worker_count; ++worker_id) {
    per_worker_outputs.emplace_back(output_column_definitions, Chunk::DEFAULT_SIZE);
  }
  {
    const auto partition_hint = cardinality_estimate / partition_count + 1;
    auto partition_cursor = std::atomic<size_t>{0};
    run_workers(merge_worker_count, [&](const size_t worker_id) {
      auto merge_map = MergeMap<KeySchema>{key_schema, shift, aggregate_schema.make_accumulator_columns()};
      auto& output = per_worker_outputs[worker_id];
      auto slots = std::vector<uint32_t>{};
      auto bitmap_tile = std::vector<std::byte>((MERGE_TILE_ROWS + 7) / 8);
      while (true) {
        const auto partition = static_cast<PartitionId>(partition_cursor.fetch_add(1, std::memory_order_relaxed));
        if (partition >= partition_count) {
          break;
        }
        merge_map.clear();
        merge_map.reserve(partition_hint);
        for (auto& store : scatter_stores) {
          const auto& key_region = store.key_region(partition);
          DebugAssert(key_region.size() % key_width == 0, "Key region must hold whole keys.");
          const auto row_count = key_region.size() / key_width;
          for (auto tile_start = size_t{0}; tile_start < row_count; tile_start += MERGE_TILE_ROWS) {
            const auto tile_rows = std::min(MERGE_TILE_ROWS, row_count - tile_start);
            slots.clear();
            merge_map.resolve({key_region.data() + tile_start * key_width, tile_rows * key_width}, slots);
            for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
              const auto stream_index = aggregate_schema.aggregate_value_stream(aggregate_index);
              if (stream_index == AggregateSchema::NO_VALUE_STREAM) {
                if (aggregate_schema.function(aggregate_index) == WindowFunction::Any) {
                  const auto& row_id_region = store.value_region(partition, row_id_stream_index);
                  merge_map.fold(aggregate_index, slots,
                                 {row_id_region.data() + tile_start * sizeof(RowID), tile_rows * sizeof(RowID)}, {});
                } else {
                  merge_map.fold(aggregate_index, slots, {}, {});
                }
                continue;
              }
              const auto width = value_stream_widths[stream_index];
              const auto& value_region = store.value_region(partition, stream_index);
              const auto value_bytes =
                  std::span<const std::byte>{value_region.data() + tile_start * width, tile_rows * width};
              auto value_null_bitmap = std::span<const std::byte>{};
              if (aggregate_schema.value_stream(stream_index).is_nullable()) {
                // Gather this stream's bits from the per-row bitmap fields into the bit-per-row tile form.
                const auto* row_bitmaps = store.value_null_bitmap_region(partition).data();
                const auto stream_bit = value_stream_null_bits[stream_index];
                std::memset(bitmap_tile.data(), 0, bitmap_tile.size());
                for (auto row = size_t{0}; row < tile_rows; ++row) {
                  const auto* row_bitmap = row_bitmaps + (tile_start + row) * value_null_bitmap_width;
                  if ((row_bitmap[stream_bit / 8] & (std::byte{1} << (stream_bit % 8))) != std::byte{0}) {
                    bitmap_tile[row / 8] |= std::byte{1} << (row % 8);
                  }
                }
                value_null_bitmap = {bitmap_tile.data(), (tile_rows + 7) / 8};
              }
              merge_map.fold(aggregate_index, slots, value_bytes, value_null_bitmap);
            }
          }
        }
        merge_map.flush_into(output);
        output.maybe_seal();
      }
      output.seal_all();
    });
  }
  step_performance_data.set_step_runtime(OperatorSteps::Merge, timer.lap());

  const auto output_table = build_output_table(output_column_definitions, per_worker_outputs);
  step_performance_data.set_step_runtime(OperatorSteps::OutputWriting, timer.lap());

  // The per-partition frees dominate a multi-threaded aggregate, so spread them over the workers. Merge is the last
  // reader and the output columns hold copies, so the stores are dead by this point.
  run_workers(scatter_worker_count, [&](const size_t worker_id) {
    scatter_stores[worker_id].release();
  });

  return output_table;
}

std::shared_ptr<Table> AggregateDYOD::_aggregate_without_group_by(const AggregateSchema& /*aggregate_schema*/,
                                                                  const Table& input_table) {
  const auto aggregators = build_aggregators(input_table, _aggregates);

  // TODO(anyone): currently morsel_count == chunk_count, this has to be adjusted
  const auto morsel_count = static_cast<size_t>(input_table.chunk_count());
  // The immediate scheduler cannot run logical workers concurrently and therefore needs only one state.
  const auto max_worker_count = Hyrise::get().is_multi_threaded() ? Hyrise::get().topology.num_cpus() : size_t{1};
  const auto worker_count = std::clamp(morsel_count, size_t{1}, max_worker_count);

  for (const auto& aggregator : aggregators) {
    aggregator->set_worker_count(worker_count);
  }

  auto morsel_cursor = std::atomic<size_t>{0};
  run_workers(worker_count, [&](const size_t worker_id) {
    while (true) {
      const auto morsel = morsel_cursor.fetch_add(1, std::memory_order_relaxed);
      if (morsel >= morsel_count) {
        break;
      }
      const auto chunk = input_table.get_chunk(ChunkID{static_cast<ChunkID::base_type>(morsel)});
      if (!chunk) {
        continue;
      }
      for (const auto& aggregator : aggregators) {
        aggregator->accumulate(worker_id, *chunk);
      }
    }
  });

  _output_column_definitions.reserve(aggregators.size());
  _output_segments.reserve(aggregators.size());
  for (const auto& aggregator : aggregators) {
    aggregator->merge();
    _output_column_definitions.push_back(aggregator->output_column_definition());
    _output_segments.push_back(aggregator->build_segment());
  }

  auto output_table = std::make_shared<Table>(_output_column_definitions, TableType::Data);
  output_table->append_chunk(_output_segments);
  return output_table;
}

std::shared_ptr<AbstractOperator> AggregateDYOD::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateDYOD>(copied_left_input, _aggregates, _groupby_column_ids);
}

void AggregateDYOD::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateDYOD::_on_cleanup() {}

}  // namespace hyrise
