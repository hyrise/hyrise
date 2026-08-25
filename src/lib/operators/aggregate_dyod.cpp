#include "aggregate_dyod.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/aggregate_schema.hpp"
#include "operators/aggregate_dyod/distinct_set.hpp"
#include "operators/aggregate_dyod/hll_sketch.hpp"
#include "operators/aggregate_dyod/key_primitives.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "operators/aggregate_dyod/merge_map.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "operators/aggregate_dyod/scatter_store.hpp"
#include "operators/aggregate_dyod/value_scatter_column.hpp"
#include "operators/operator_performance_data.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/attribute_statistics.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace hyrise {

namespace {

class AbstractAggregator {
 public:
  virtual ~AbstractAggregator() = default;
  virtual void set_worker_count(size_t worker_count) = 0;
  virtual void accumulate(size_t worker_id, ChunkID chunk_id, const Chunk& chunk) = 0;
  virtual void merge() = 0;
  virtual std::shared_ptr<AbstractSegment> build_segment() const = 0;
  virtual TableColumnDefinition output_column_definition() const = 0;
};

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

// The unit a scanning phase claims at a time: a row range of one chunk.
struct MorselJob {
  ChunkID chunk_id;
  ChunkOffset row_begin;
  ChunkOffset row_end;
};

// Splitting chunks into morsels decouples the parallelism from how the input happens to be chunked. Single-threaded
// runs get one job per chunk, since a split only repeats the per-morsel setup; `chunk_stride` skips chunks so the
// estimate phase can enumerate its sample.
std::vector<MorselJob> build_morsel_jobs(const Table& input_table, const size_t worker_limit,
                                         const size_t chunk_stride) {
  const auto chunk_count = static_cast<size_t>(input_table.chunk_count());
  auto jobs = std::vector<MorselJob>{};
  jobs.reserve((chunk_count + chunk_stride - 1) / chunk_stride);
  for (auto chunk_index = size_t{0}; chunk_index < chunk_count; chunk_index += chunk_stride) {
    const auto chunk_id = ChunkID{static_cast<ChunkID::base_type>(chunk_index)};
    const auto chunk = input_table.get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }
    const auto row_count = size_t{chunk->size()};
    if (row_count == 0) {
      continue;
    }

    const auto rows_per_morsel = worker_limit > 1 ? MORSEL_ROWS : row_count;
    const auto morsel_count = morsel_count_for(row_count, rows_per_morsel);
    for (auto morsel = size_t{0}; morsel < morsel_count; ++morsel) {
      const auto row_begin = morsel * rows_per_morsel;
      const auto row_end = std::min(row_begin + rows_per_morsel, row_count);
      jobs.emplace_back(MorselJob{.chunk_id = chunk_id,
                                  .row_begin = ChunkOffset{static_cast<ChunkOffset::base_type>(row_begin)},
                                  .row_end = ChunkOffset{static_cast<ChunkOffset::base_type>(row_end)}});
    }
  }
  return jobs;
}

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
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      _decode_scratch.resize(worker_count);
    }
  }

  void accumulate(const size_t worker_id, const ChunkID /*chunk_id*/, const Chunk& chunk) override {
    auto& state = _states[worker_id].state;
    const auto fold = WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>{}.get_aggregate_function();

    if constexpr (window_function == WindowFunction::Min || window_function == WindowFunction::Max) {
      const auto pruning_statistics = chunk.pruning_statistics();
      if (pruning_statistics && static_cast<size_t>(_column_id) < pruning_statistics->size()) {
        DebugAssert((*pruning_statistics)[_column_id]->data_type() == data_type_from_type<ColumnDataType>(),
                    "Pruning statistics do not match the column type.");
        const auto& attribute_statistics =
            static_cast<const AttributeStatistics<ColumnDataType>&>(*(*pruning_statistics)[_column_id]);

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

    // Hold the shared_ptr while scanning; a concurrent ChunkEncoder may swap the segment out.
    const auto segment = chunk.get_segment(_column_id);
    if constexpr (std::is_same_v<ColumnDataType, pmr_string> &&
                  (window_function == WindowFunction::Min || window_function == WindowFunction::Max)) {
      // segment_iterate materializes every string by value; decoded views skip the per-row copies.
      auto& decoded = _decode_scratch[worker_id];
      decode_string_column(*segment, decoded);
      auto accumulator = std::move(state.accumulator);
      auto count = state.count;
      const auto row_count = decoded.values.size();
      for (auto row = size_t{0}; row < row_count; ++row) {
        if (decoded.nulls[row] != 0) {
          continue;
        }
        const auto value = decoded.values[row];
        if (count == 0 || (window_function == WindowFunction::Min ? value < accumulator : value > accumulator)) {
          accumulator = pmr_string{value};
        }
        ++count;
      }
      state.accumulator = std::move(accumulator);
      state.count = count;
      return;
    }

    auto accumulator = state.accumulator;
    auto count = state.count;
    segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
      if (position.is_null()) {
        return;
      }
      fold(position.value(), count, accumulator);
      ++count;
    });
    state.accumulator = std::move(accumulator);
    state.count = count;
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
  std::vector<KeyDecodeScratch::StringColumn> _decode_scratch;
  State _final{};
};

template <typename ColumnDataType>
class AnyAggregator : public AbstractAggregator {
  struct State {
    AllTypeVariant value;
    ChunkID chunk_id{0};
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

  void accumulate(const size_t worker_id, const ChunkID chunk_id, const Chunk& chunk) override {
    auto& state = _states[worker_id].state;
    if (state.seen || chunk.size() == 0) {
      return;
    }
    state.value = (*chunk.get_segment(_column_id))[ChunkOffset{0}];
    state.chunk_id = chunk_id;
    state.seen = true;
  }

  void merge() override {
    // Take the row from the lowest chunk across workers so ANY is the first row of the first non-empty chunk,
    // independent of how the scheduler handed chunks to workers.
    for (const auto& padded : _states) {
      const auto& state = padded.state;
      if (state.seen && (!_final.seen || state.chunk_id < _final.chunk_id)) {
        _final = state;
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

  void accumulate(const size_t worker_id, const ChunkID /*chunk_id*/, const Chunk& chunk) override {
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

  void accumulate(const size_t worker_id, const ChunkID /*chunk_id*/, const Chunk& chunk) override {
    auto count = _states[worker_id].count;
    segment_iterate<ColumnDataType>(*chunk.get_segment(_column_id), [&](const auto& position) {
      if (!position.is_null()) {
        ++count;
      }
    });
    _states[worker_id].count = count;
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
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      _decode_scratch.resize(worker_count);
    }
  }

  void accumulate(const size_t worker_id, const ChunkID /*chunk_id*/, const Chunk& chunk) override {
    auto& set = _states[worker_id].set;
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      auto& decoded = _decode_scratch[worker_id];
      const auto segment = chunk.get_segment(_column_id);
      decode_string_column(*segment, decoded);
      const auto row_count = decoded.values.size();
      for (auto row = size_t{0}; row < row_count; ++row) {
        if (decoded.nulls[row] == 0) {
          set.insert(0, decoded.values[row]);
        }
      }
      return;
    }

    segment_iterate<ColumnDataType>(*chunk.get_segment(_column_id), [&](const auto& position) {
      if (position.is_null()) {
        return;
      }
      set.insert(0, position.value());
    });
  }

  void merge() override {
    const auto worker_count = _states.size();
    if (worker_count == 1) {
      _final_count = static_cast<int64_t>(_states.front().set.size());
      return;
    }

    const auto range_count = std::bit_ceil(worker_count);
    auto buckets = std::vector<std::vector<DistinctSet<ColumnDataType>>>(worker_count);
    run_workers(worker_count, [&](const size_t worker_id) {
      buckets[worker_id] = std::vector<DistinctSet<ColumnDataType>>(range_count);
      _states[worker_id].set.split_into(buckets[worker_id]);
    });

    auto range_counts = std::vector<size_t>(range_count, 0);
    run_workers(range_count, [&](const size_t range_index) {
      auto& merged = buckets.front()[range_index];
      for (auto worker_id = size_t{1}; worker_id < worker_count; ++worker_id) {
        merged.merge(buckets[worker_id][range_index]);
      }
      range_counts[range_index] = merged.size();
    });

    _final_count = 0;
    for (const auto count : range_counts) {
      _final_count += static_cast<int64_t>(count);
    }
  }

  std::shared_ptr<AbstractSegment> build_segment() const override {
    return std::make_shared<ValueSegment<int64_t>>(pmr_vector{_final_count});
  }

  TableColumnDefinition output_column_definition() const override {
    return TableColumnDefinition{_output_name, DataType::Long, false};
  }

 private:
  std::string _output_name;
  ColumnID _column_id;
  std::vector<PaddedState> _states;
  std::vector<KeyDecodeScratch::StringColumn> _decode_scratch;
  int64_t _final_count{0};
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

void gather_value_column(const AbstractSegment& segment, const DataType type, const bool nullable,
                         std::vector<std::byte>& out_bytes, std::vector<std::byte>& out_null, const size_t row_begin,
                         const size_t row_end) {
  const auto row_count = row_end - row_begin;
  resolve_data_type(type, [&](const auto data_type) {
    using ColumnDataType = typename decltype(data_type)::type;
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      Fail("Unexpected string column.");
    } else {
      out_bytes.assign(row_count * sizeof(ColumnDataType), std::byte{0});
      if (nullable) {
        out_null.assign((row_count + 7) / 8, std::byte{0});
      }
      iterate_segment_window<ColumnDataType>(
          segment, row_begin, row_end, [&](const size_t row, const ColumnDataType* value) {
            if (!value) {
              if (nullable) {
                out_null[row / 8] |= std::byte{1} << (row % 8);
              }
              return;
            }
            std::memcpy(out_bytes.data() + (row * sizeof(ColumnDataType)), value, sizeof(ColumnDataType));
          });
    }
  });
}

// String cells are gathered as (pointer, length) references into `values`, which must stay alive through the fold.
void gather_string_value_column(const AbstractSegment& segment, const bool nullable, std::vector<pmr_string>& values,
                                std::vector<std::byte>& out_bytes, std::vector<std::byte>& out_null,
                                const size_t row_begin, const size_t row_end) {
  const auto row_count = row_end - row_begin;
  values.resize(row_count);
  out_bytes.assign(row_count * sizeof(StringValueReference), std::byte{0});
  if (nullable) {
    out_null.assign((row_count + 7) / 8, std::byte{0});
  }
  iterate_segment_window<pmr_string>(segment, row_begin, row_end, [&](const size_t row, const pmr_string* source) {
    if (!source) {
      if (nullable) {
        out_null[row / 8] |= std::byte{1} << (row % 8);
      }
      return;
    }
    auto& value = values[row];
    value = *source;
    const auto reference =
        StringValueReference{.data = reinterpret_cast<const std::byte*>(value.data()), .length = value.size()};
    std::memcpy(out_bytes.data() + (row * sizeof(reference)), &reference, sizeof(reference));
  });
}

// Collects one chunk's group-by segments: `owners` keeps the shared_ptrs alive while `segments` holds the raw views the
// key schema decodes from. Shared by the estimate, scatter, and low-cardinality accumulation passes.
void gather_group_by_segments(const Chunk& chunk, const std::vector<ColumnID>& groupby_column_ids,
                              std::vector<std::shared_ptr<AbstractSegment>>& owners,
                              std::vector<const AbstractSegment*>& segments) {
  owners.clear();
  segments.clear();
  for (const auto column_id : groupby_column_ids) {
    owners.emplace_back(chunk.get_segment(column_id));
    segments.emplace_back(owners.back().get());
  }
}

// Builds the result table's column definitions: the group-by columns first, then one column per aggregate. ANY carries
// its input column's name and nullability; every other function names its column after the aggregate expression and is
// nullable unless it is COUNT or COUNT(DISTINCT).
TableColumnDefinitions build_output_column_definitions(
    const Table& input_table, const std::vector<ColumnID>& groupby_column_ids, const AggregateSchema& aggregate_schema,
    const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates) {
  const auto aggregate_count = aggregate_schema.aggregate_count();
  auto output_column_definitions = TableColumnDefinitions{};
  output_column_definitions.reserve(groupby_column_ids.size() + aggregate_count);
  for (const auto column_id : groupby_column_ids) {
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
        aggregates[aggregate_index]->as_column_name(), aggregate_schema.result_type(aggregate_index),
        function != WindowFunction::Count && function != WindowFunction::CountDistinct);
  }
  return output_column_definitions;
}

// Estimate phase: per-worker HyperLogLog sketches over the packed-key hash of every sample_stride-th chunk choose the
// query's cardinality estimate. The sample is selected per chunk; the morsels only split the scan of a sampled chunk.
// When striding, a half-sample sketch lets scale_sampled_estimate rescale the strided estimate up to the full table.
template <typename KeySchema>
size_t estimate_cardinality(const KeySchema& key_schema, const Table& input_table,
                            const std::vector<ColumnID>& groupby_column_ids, const size_t worker_limit,
                            const size_t chunk_count, const size_t key_width) {
  const auto sample_stride = estimate_sample_stride(chunk_count);
  const auto estimate_worker_limit = input_table.row_count() < PARALLEL_ESTIMATE_THRESHOLD ? size_t{1} : worker_limit;
  const auto estimate_jobs = build_morsel_jobs(input_table, estimate_worker_limit, sample_stride);
  const auto estimate_job_count = estimate_jobs.size();
  const auto estimate_worker_count = std::clamp(estimate_job_count, size_t{1}, estimate_worker_limit);
  auto sketches = std::vector<HllSketch>(estimate_worker_count);
  // The half-sample sketch only exists to rescale a strided sample.
  auto half_sketches = std::vector<HllSketch>(sample_stride > 1 ? estimate_worker_count : 0);
  {
    auto job_cursor = std::atomic<size_t>{0};
    run_workers(estimate_worker_count, [&](const size_t worker_id) {
      auto& sketch = sketches[worker_id];
      auto key_scratch = std::vector<std::byte>(key_width);
      auto decode_scratch = KeyDecodeScratch{};
      auto spill_scratch = StringSpillBuffer{};
      auto segment_owners = std::vector<std::shared_ptr<AbstractSegment>>{};
      auto segments = std::vector<const AbstractSegment*>{};
      while (true) {
        const auto job_index = job_cursor.fetch_add(1, std::memory_order_relaxed);
        if (job_index >= estimate_job_count) {
          break;
        }
        const auto& job = estimate_jobs[job_index];
        const auto chunk = input_table.get_chunk(job.chunk_id);
        const auto in_half_sample = sample_stride > 1 && (size_t{job.chunk_id} / sample_stride) % 2 == 0;
        gather_group_by_segments(*chunk, groupby_column_ids, segment_owners, segments);
        key_schema.decode(segments, job.row_begin, job.row_end, decode_scratch);
        const auto row_count = job.row_end - job.row_begin;
        for (auto morsel_offset = ChunkOffset{0}; morsel_offset < row_count; ++morsel_offset) {
          key_schema.pack(decode_scratch, morsel_offset, key_scratch.data(), spill_scratch);
          const auto key_hash = key_schema.hash(key_scratch.data());
          sketch.add(key_hash);
          if (in_half_sample) {
            half_sketches[worker_id].add(key_hash);
          }
        }
        spill_scratch.clear();
      }
    });
  }
  for (auto worker_id = size_t{1}; worker_id < estimate_worker_count; ++worker_id) {
    sketches.front().merge(sketches[worker_id]);
    if (sample_stride > 1) {
      half_sketches.front().merge(half_sketches[worker_id]);
    }
  }
  return scale_sampled_estimate(sketches.front().estimate(),
                                sample_stride > 1 ? half_sketches.front().estimate() : size_t{0},
                                input_table.row_count(), sample_stride);
}

// The per-query scatter layout: the value-stream metadata plus the SWWC stream widths derived from it. Computed once
// from the AggregateSchema and shared, read-only, by the scatter and merge phases.
struct ScatterLayout {
  std::vector<size_t> value_stream_widths;
  std::vector<ColumnID> value_stream_sources;
  std::vector<uint32_t> value_stream_null_bits;
  size_t value_stream_count{0};
  uint32_t value_null_bitmap_width{0};
  bool has_value_null_bitmap{false};
  bool needs_value_arena{false};
  bool needs_row_id_stream{false};
  size_t row_id_stream_index{0};
  size_t value_null_bitmap_stream_index{0};
  size_t piece_width{0};
  std::vector<size_t> stream_widths;
};

// Derives the scatter/merge layout from the aggregate schema and packed key width. The packed key is staged in
// key_piece_width() pieces and the value-null bitmap byte-wise, so every declared stream width divides the SWWC line.
ScatterLayout compute_scatter_layout(const AggregateSchema& aggregate_schema, const size_t key_width) {
  auto layout = ScatterLayout{};
  const auto value_stream_count = aggregate_schema.value_stream_count();
  layout.value_stream_count = value_stream_count;
  layout.value_stream_widths = std::vector<size_t>(value_stream_count);
  layout.value_stream_sources = std::vector<ColumnID>(value_stream_count, INVALID_COLUMN_ID);
  layout.value_stream_null_bits = std::vector<uint32_t>(value_stream_count, 0);
  auto nullable_stream_count = uint32_t{0};
  for (auto stream_index = size_t{0}; stream_index < value_stream_count; ++stream_index) {
    const auto& stream = aggregate_schema.value_stream(stream_index);
    layout.value_stream_widths[stream_index] = stream.element_width();
    if (stream.is_nullable()) {
      layout.value_stream_null_bits[stream_index] = nullable_stream_count;
      ++nullable_stream_count;
    }
  }
  const auto aggregate_count = aggregate_schema.aggregate_count();
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto stream_index = aggregate_schema.aggregate_value_stream(aggregate_index);
    if (stream_index == AggregateSchema::NO_VALUE_STREAM) {
      continue;
    }
    layout.value_stream_sources[stream_index] = aggregate_schema.source_column(aggregate_index);
  }

  layout.value_null_bitmap_width = aggregate_schema.value_null_bitmap_width();
  layout.has_value_null_bitmap = layout.value_null_bitmap_width > 0;
  layout.needs_value_arena = aggregate_schema.needs_value_arena();

  // ANY aggregates scatter no value; they read one shared row-id stream instead.
  layout.needs_row_id_stream = aggregate_schema.needs_row_id_stream();
  layout.row_id_stream_index = value_stream_count;
  if (layout.needs_row_id_stream) {
    layout.value_stream_widths.emplace_back(sizeof(RowID));
  }
  layout.value_null_bitmap_stream_index = 1 + layout.value_stream_widths.size();

  layout.piece_width = key_piece_width(key_width);
  layout.stream_widths.reserve(1 + layout.value_stream_widths.size() + (layout.has_value_null_bitmap ? 1 : 0));
  layout.stream_widths.emplace_back(layout.piece_width);
  layout.stream_widths.insert(layout.stream_widths.end(), layout.value_stream_widths.begin(),
                              layout.value_stream_widths.end());
  if (layout.has_value_null_bitmap) {
    layout.stream_widths.emplace_back(1);
  }
  return layout;
}

// Scatter phase: each worker buffers raw (key, values...) rows into its own store across the partitions. The key pass
// packs and routes each row and records its partition; the value streams, the optional row-id stream, and the
// value-null bitmap then run as separate column-wise passes over the same morsel.
template <typename KeySchema>
void run_scatter_phase(const KeySchema& key_schema, const AggregateSchema& aggregate_schema, const Table& input_table,
                       const std::vector<ColumnID>& groupby_column_ids, const ScatterLayout& layout,
                       std::vector<ScatterStore>& scatter_stores, const std::vector<MorselJob>& scatter_jobs,
                       const size_t scatter_job_count, const size_t scatter_worker_count,
                       const PartitionCount partition_count, const size_t key_width) {
  auto job_cursor = std::atomic<size_t>{0};
  run_workers(scatter_worker_count, [&](const size_t worker_id) {
    auto& store = scatter_stores[worker_id];
    auto heads =
        ScatterHeads{partition_count, layout.stream_widths.size(), layout.stream_widths, layout.has_value_null_bitmap};
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
      const auto job_index = job_cursor.fetch_add(1, std::memory_order_relaxed);
      if (job_index >= scatter_job_count) {
        break;
      }
      const auto& job = scatter_jobs[job_index];
      const auto chunk = input_table.get_chunk(job.chunk_id);
      gather_group_by_segments(*chunk, groupby_column_ids, segment_owners, segments);
      value_segment_owners.clear();
      value_segments.clear();
      for (const auto column_id : layout.value_stream_sources) {
        value_segment_owners.emplace_back(chunk->get_segment(column_id));
        value_segments.emplace_back(value_segment_owners.back().get());
      }
      const auto row_count = job.row_end - job.row_begin;

      key_schema.decode(segments, job.row_begin, job.row_end, decode_scratch);
      row_partitions.resize(row_count);
      for (auto morsel_offset = ChunkOffset{0}; morsel_offset < row_count; ++morsel_offset) {
        key_schema.pack(decode_scratch, morsel_offset, key_scratch.data(), pack_spill);
        const auto key_hash = key_schema.hash(key_scratch.data());
        const auto partition = static_cast<PartitionId>(key_hash & (partition_count - 1));
        row_partitions[morsel_offset] = partition;
        if constexpr (KeySchema::HAS_STRINGS) {
          key_schema.reintern_spill(key_scratch.data(), store.key_spill_buffer(partition));
          pack_spill.clear();
        }
        for (auto piece_offset = size_t{0}; piece_offset < key_width; piece_offset += layout.piece_width) {
          heads.push(store, 0, partition, key_scratch.data() + piece_offset, layout.piece_width);
        }
      }

      if (layout.has_value_null_bitmap) {
        bitmap_scratch.assign(size_t{row_count} * layout.value_null_bitmap_width, std::byte{0});
      }
      for (auto stream_index = size_t{0}; stream_index < layout.value_stream_count; ++stream_index) {
        aggregate_schema.value_stream(stream_index)
            .scatter(*value_segments[stream_index], job.row_begin, job.row_end, row_partitions, 1 + stream_index, heads,
                     store, bitmap_scratch.data(), layout.value_null_bitmap_width,
                     layout.value_stream_null_bits[stream_index]);
      }
      if (layout.needs_row_id_stream) {
        for (auto morsel_offset = ChunkOffset{0}; morsel_offset < row_count; ++morsel_offset) {
          const auto row_id = RowID{job.chunk_id, ChunkOffset{job.row_begin + morsel_offset}};
          heads.push(store, 1 + layout.row_id_stream_index, row_partitions[morsel_offset],
                     reinterpret_cast<const std::byte*>(&row_id), sizeof(row_id));
        }
      }
      if (layout.has_value_null_bitmap) {
        for (auto morsel_offset = ChunkOffset{0}; morsel_offset < row_count; ++morsel_offset) {
          const auto* row_bitmap = bitmap_scratch.data() + (size_t{morsel_offset} * layout.value_null_bitmap_width);
          for (auto byte_index = size_t{0}; byte_index < layout.value_null_bitmap_width; ++byte_index) {
            heads.push(store, layout.value_null_bitmap_stream_index, row_partitions[morsel_offset],
                       row_bitmap + byte_index, 1);
          }
        }
      }
    }
    heads.finish(store);
  });
}

// Merge phase: workers claim jobs and fold one partition's rows from a range of the stores through a dense MergeMap.
// Returns the per-worker OutputColumns the caller assembles into the result table.
template <typename KeySchema>
std::vector<OutputColumns> run_merge_phase(const KeySchema& key_schema, const AggregateSchema& aggregate_schema,
                                           const ScatterLayout& layout, std::vector<ScatterStore>& scatter_stores,
                                           const TableColumnDefinitions& output_column_definitions,
                                           const PartitionCount partition_count, const uint32_t shift,
                                           const size_t key_width, const size_t cardinality_estimate,
                                           const size_t worker_limit, const size_t aggregate_count) {
  auto partition_rows = std::vector<size_t>(partition_count, 0);
  auto scattered_row_count = size_t{0};
  for (auto& store : scatter_stores) {
    for (auto partition = PartitionId{0}; partition < partition_count; ++partition) {
      const auto rows = store.key_region(partition).size() / key_width;
      partition_rows[partition] += rows;
      scattered_row_count += rows;
    }
  }

  struct MergeJob {
    PartitionId partition;
    size_t first_store;
    size_t last_store;
    size_t split_index;  // Index into split_partitions, or NO_SPLIT for a job covering every store.
    size_t split_way;    // This job's slot among its partition's sub-jobs.
  };

  // A split partition's sub-jobs publish their maps here and count down; the last one combines them and emits.
  struct SplitPartition {
    std::vector<std::unique_ptr<MergeMap<KeySchema>>> maps;
    std::atomic<size_t> remaining;
  };

  constexpr auto NO_SPLIT = std::numeric_limits<size_t>::max();
  const auto store_count = scatter_stores.size();
  const auto split_eligible = merge_split_eligible(aggregate_schema);
  auto merge_jobs = std::vector<MergeJob>{};
  merge_jobs.reserve(partition_count);
  auto split_partitions = std::vector<std::unique_ptr<SplitPartition>>{};
  for (auto partition = PartitionId{0}; partition < partition_count; ++partition) {
    const auto split_ways =
        split_eligible ? merge_split_ways_for(partition_rows[partition], scattered_row_count / partition_count,
                                              cardinality_estimate / partition_count, store_count, worker_limit)
                       : size_t{1};
    if (split_ways == 1) {
      merge_jobs.emplace_back(MergeJob{partition, 0, store_count, NO_SPLIT, 0});
      continue;
    }

    const auto split_index = split_partitions.size();
    auto& split = *split_partitions.emplace_back(std::make_unique<SplitPartition>());
    split.maps.resize(split_ways);
    split.remaining.store(split_ways, std::memory_order_relaxed);
    for (auto way = size_t{0}; way < split_ways; ++way) {
      merge_jobs.emplace_back(
          MergeJob{partition, way * store_count / split_ways, (way + 1) * store_count / split_ways, split_index, way});
    }
  }

  const auto job_count = merge_jobs.size();
  const auto merge_worker_count = std::min(job_count, worker_limit);
  auto per_worker_outputs = std::vector<OutputColumns>{};
  per_worker_outputs.reserve(merge_worker_count);
  for (auto worker_id = size_t{0}; worker_id < merge_worker_count; ++worker_id) {
    per_worker_outputs.emplace_back(output_column_definitions, Chunk::DEFAULT_SIZE);
  }
  {
    const auto partition_hint = (cardinality_estimate / partition_count) + 1;
    auto job_cursor = std::atomic<size_t>{0};
    run_workers(merge_worker_count, [&](const size_t worker_id) {
      auto merge_map = MergeMap<KeySchema>{key_schema, shift, aggregate_schema.make_accumulator_columns()};
      auto& output = per_worker_outputs[worker_id];
      auto slots = std::vector<uint32_t>{};
      auto bitmap_tile = std::vector<std::byte>((merge_tile_rows() + 7) / 8);

      const auto fold_store_range = [&](MergeMap<KeySchema>& map, const MergeJob& job) {
        for (auto store_index = job.first_store; store_index < job.last_store; ++store_index) {
          auto& store = scatter_stores[store_index];
          const auto& key_region = store.key_region(job.partition);
          DebugAssert(key_region.size() % key_width == 0, "Key region must hold whole keys.");
          const auto row_count = key_region.size() / key_width;
          const auto max_tile_rows = merge_tile_rows();
          for (auto tile_start = size_t{0}; tile_start < row_count; tile_start += max_tile_rows) {
            const auto tile_rows = std::min(max_tile_rows, row_count - tile_start);
            slots.clear();
            map.resolve({key_region.data() + (tile_start * key_width), tile_rows * key_width}, slots);
            for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
              const auto stream_index = aggregate_schema.aggregate_value_stream(aggregate_index);
              if (stream_index == AggregateSchema::NO_VALUE_STREAM) {
                if (aggregate_schema.function(aggregate_index) == WindowFunction::Any) {
                  const auto& row_id_region = store.value_region(job.partition, layout.row_id_stream_index);
                  map.fold(aggregate_index, slots,
                           {row_id_region.data() + (tile_start * sizeof(RowID)), tile_rows * sizeof(RowID)}, {});
                } else {
                  map.fold(aggregate_index, slots, {}, {});
                }
                continue;
              }
              const auto width = layout.value_stream_widths[stream_index];
              const auto& value_region = store.value_region(job.partition, stream_index);
              const auto value_bytes =
                  std::span<const std::byte>{value_region.data() + (tile_start * width), tile_rows * width};
              auto value_null_bitmap = std::span<const std::byte>{};
              if (aggregate_schema.value_stream(stream_index).is_nullable()) {
                // Gather this stream's bits from the per-row bitmap fields into the bit-per-row tile form.
                const auto* row_bitmaps = store.value_null_bitmap_region(job.partition).data();
                const auto stream_bit = layout.value_stream_null_bits[stream_index];
                std::memset(bitmap_tile.data(), 0, bitmap_tile.size());
                for (auto row = size_t{0}; row < tile_rows; ++row) {
                  const auto* row_bitmap = row_bitmaps + ((tile_start + row) * layout.value_null_bitmap_width);
                  if ((row_bitmap[stream_bit / 8] & (std::byte{1} << (stream_bit % 8))) != std::byte{0}) {
                    bitmap_tile[row / 8] |= std::byte{1} << (row % 8);
                  }
                }
                value_null_bitmap = {bitmap_tile.data(), (tile_rows + 7) / 8};
              }
              map.fold(aggregate_index, slots, value_bytes, value_null_bitmap);
            }
          }
        }
      };

      while (true) {
        const auto job_index = job_cursor.fetch_add(1, std::memory_order_relaxed);
        if (job_index >= job_count) {
          break;
        }
        const auto& job = merge_jobs[job_index];
        if (job.split_index == NO_SPLIT) {
          merge_map.clear();
          merge_map.reserve(partition_hint);
          fold_store_range(merge_map, job);
          merge_map.flush_into(output);
          output.maybe_seal();
          continue;
        }

        auto& split = *split_partitions[job.split_index];
        auto sub_map =
            std::make_unique<MergeMap<KeySchema>>(key_schema, shift, aggregate_schema.make_accumulator_columns());
        sub_map->reserve(partition_hint);
        fold_store_range(*sub_map, job);
        split.maps[job.split_way] = std::move(sub_map);
        // The sub-maps stay alive until the phase ends: a combined string key may point into their spill buffers.
        if (split.remaining.fetch_sub(1, std::memory_order_acq_rel) > 1) {
          continue;
        }
        auto& combined = *split.maps[job.split_way];
        const auto way_count = split.maps.size();
        for (auto way = size_t{0}; way < way_count; ++way) {
          if (way != job.split_way) {
            combined.combine(*split.maps[way]);
          }
        }
        combined.flush_into(output);
        output.maybe_seal();
      }
      output.seal_all();
    });
  }
  return per_worker_outputs;
}

// Low-cardinality accumulation: each worker folds its claimed morsels straight into its own private MergeMap, skipping
// the scatter partitioning entirely.
template <typename KeySchema>
void accumulate_private_maps(const KeySchema& key_schema, const AggregateSchema& aggregate_schema,
                             const Table& input_table, const std::vector<ColumnID>& groupby_column_ids,
                             std::vector<MergeMap<KeySchema>>& per_worker_private_maps,
                             const std::vector<MorselJob>& morsel_jobs, const size_t job_count,
                             const size_t worker_count, const size_t key_width, const size_t aggregate_count,
                             const size_t cardinality_estimate) {
  auto job_cursor = std::atomic<size_t>{0};
  run_workers(worker_count, [&](const size_t worker_id) {
    auto& merge_map = per_worker_private_maps[worker_id];
    merge_map.reserve(cardinality_estimate);

    auto decode_scratch = KeyDecodeScratch{};
    auto spill_scratch = StringSpillBuffer{};
    auto key_buffer = std::vector<std::byte>{};
    auto slots = std::vector<uint32_t>{};
    auto value_buffers = std::vector<std::vector<std::byte>>(aggregate_count);
    auto null_buffers = std::vector<std::vector<std::byte>>(aggregate_count);
    auto string_holders = std::vector<std::vector<pmr_string>>(aggregate_count);
    auto owners = std::vector<std::shared_ptr<AbstractSegment>>{};
    auto segments = std::vector<const AbstractSegment*>{};

    while (true) {
      const auto job_index = job_cursor.fetch_add(1, std::memory_order_relaxed);
      if (job_index >= job_count) {
        break;
      }
      const auto& job = morsel_jobs[job_index];
      const auto chunk = input_table.get_chunk(job.chunk_id);
      const auto row_count = job.row_end - job.row_begin;

      gather_group_by_segments(*chunk, groupby_column_ids, owners, segments);
      key_schema.decode(segments, job.row_begin, job.row_end, decode_scratch);
      key_buffer.resize(size_t{row_count} * key_width);
      for (auto offset = ChunkOffset{0}; offset < row_count; ++offset) {
        key_schema.pack(decode_scratch, offset, key_buffer.data() + (size_t{offset} * key_width), spill_scratch);
      }

      for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
        const auto stream = aggregate_schema.aggregate_value_stream(aggregate_index);
        if (stream == AggregateSchema::NO_VALUE_STREAM) {
          continue;
        }
        const auto source = aggregate_schema.source_column(aggregate_index);
        const auto nullable = aggregate_schema.value_stream(stream).is_nullable();
        const auto source_type = input_table.column_data_type(source);
        if (source_type == DataType::String) {
          gather_string_value_column(*chunk->get_segment(source), nullable, string_holders[aggregate_index],
                                     value_buffers[aggregate_index], null_buffers[aggregate_index], job.row_begin,
                                     job.row_end);
        } else {
          gather_value_column(*chunk->get_segment(source), source_type, nullable, value_buffers[aggregate_index],
                              null_buffers[aggregate_index], job.row_begin, job.row_end);
        }
      }

      const auto max_tile_rows = merge_tile_rows();
      DebugAssert(max_tile_rows % 8 == 0, "Tile boundaries must fall on null-bitmap byte boundaries.");
      for (auto tile_start = size_t{0}; tile_start < row_count; tile_start += max_tile_rows) {
        const auto tile_rows = std::min(max_tile_rows, size_t{row_count} - tile_start);
        slots.clear();
        merge_map.resolve({key_buffer.data() + (tile_start * key_width), tile_rows * key_width}, slots);

        for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
          const auto stream = aggregate_schema.aggregate_value_stream(aggregate_index);
          if (stream == AggregateSchema::NO_VALUE_STREAM) {
            merge_map.fold(aggregate_index, slots, {}, {});
            continue;
          }
          const auto width = aggregate_schema.value_stream(stream).element_width();
          const auto nullable = aggregate_schema.value_stream(stream).is_nullable();
          const auto value_span = std::span<const std::byte>{
              value_buffers[aggregate_index].data() + (tile_start * width), tile_rows * width};
          auto null_span = std::span<const std::byte>{};
          if (nullable) {
            null_span = {null_buffers[aggregate_index].data() + (tile_start / 8), (tile_rows + 7) / 8};
          }
          merge_map.fold(aggregate_index, slots, value_span, null_span);
        }
      }
      spill_scratch.clear();
    }
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

  const auto worker_limit = worker_limit_for(Hyrise::get().is_multi_threaded(), Hyrise::get().topology.num_cpus());
  const auto morsel_jobs = build_morsel_jobs(input_table, worker_limit, 1);
  const auto job_count = morsel_jobs.size();
  const auto worker_count = std::clamp(job_count, size_t{1}, worker_limit);
  const auto aggregate_count = aggregate_schema.aggregate_count();
  const auto key_width = key_schema.packed_width();

  auto per_worker_private_maps = std::vector<MergeMap<KeySchema>>{};
  per_worker_private_maps.reserve(worker_count);
  for (auto worker = size_t{0}; worker < worker_count; ++worker) {
    // No radix partitioning on this path, so the map probes on the full hash instead of shifting out partition bits.
    per_worker_private_maps.emplace_back(key_schema, uint32_t{0}, aggregate_schema.make_accumulator_columns());
  }

  accumulate_private_maps(key_schema, aggregate_schema, input_table, _groupby_column_ids, per_worker_private_maps,
                          morsel_jobs, job_count, worker_count, key_width, aggregate_count, cardinality_estimate);
  step_performance_data.set_step_runtime(OperatorSteps::Scatter, timer.lap());

  // Reduce the per-worker private maps into map 0, then emit its groups as the result.
  {
    auto& combined = per_worker_private_maps.front();
    for (auto worker = size_t{1}; worker < worker_count; ++worker) {
      combined.combine(per_worker_private_maps[worker]);
    }
  }

  const auto output_column_definitions =
      build_output_column_definitions(input_table, _groupby_column_ids, aggregate_schema, _aggregates);

  auto per_worker_outputs = std::vector<OutputColumns>{};
  per_worker_outputs.emplace_back(output_column_definitions, Chunk::DEFAULT_SIZE);
  {
    per_worker_private_maps.front().flush_into(per_worker_outputs.front());
    per_worker_outputs.front().seal_all();
  }
  step_performance_data.set_step_runtime(OperatorSteps::Merge, timer.lap());

  auto output_table = build_output_table(output_column_definitions, per_worker_outputs);
  step_performance_data.set_step_runtime(OperatorSteps::OutputWriting, timer.lap());
  return output_table;
}

template <typename KeySchema>
std::shared_ptr<Table> AggregateDYOD::_aggregate(const KeySchema& key_schema, const AggregateSchema& aggregate_schema,
                                                 const Table& input_table) {
  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  auto timer = Timer{};

  const auto chunk_count = static_cast<size_t>(input_table.chunk_count());
  const auto worker_limit = worker_limit_for(Hyrise::get().is_multi_threaded(), Hyrise::get().topology.num_cpus());
  const auto scatter_jobs = build_morsel_jobs(input_table, worker_limit, 1);
  const auto scatter_job_count = scatter_jobs.size();
  const auto scatter_worker_count = std::clamp(scatter_job_count, size_t{1}, worker_limit);
  const auto key_width = key_schema.packed_width();

  const auto cardinality_estimate =
      estimate_cardinality(key_schema, input_table, _groupby_column_ids, worker_limit, chunk_count, key_width);
  step_performance_data.set_step_runtime(OperatorSteps::Estimate, timer.lap());

  if (cardinality_estimate <= low_cardinality_threshold() && low_cardinality_eligible(aggregate_schema)) {
    return _aggregate_low_cardinality(key_schema, aggregate_schema, input_table, cardinality_estimate);
  }

  const auto layout = compute_scatter_layout(aggregate_schema, key_width);
  const auto aggregate_count = aggregate_schema.aggregate_count();

  // The ceiling on P depends on the query's stream count, so the partition count is only chosen here.
  const auto partition_count = choose_partition_count(cardinality_estimate, worker_limit, layout.stream_widths.size());
  const auto shift = static_cast<uint32_t>(std::countr_zero(partition_count));

  auto scatter_stores = std::vector<ScatterStore>{};
  scatter_stores.reserve(scatter_worker_count);
  for (auto worker_id = size_t{0}; worker_id < scatter_worker_count; ++worker_id) {
    scatter_stores.emplace_back(partition_count, key_width, layout.value_stream_widths, layout.value_null_bitmap_width,
                                layout.needs_value_arena);
  }

  run_scatter_phase(key_schema, aggregate_schema, input_table, _groupby_column_ids, layout, scatter_stores,
                    scatter_jobs, scatter_job_count, scatter_worker_count, partition_count, key_width);
  step_performance_data.set_step_runtime(OperatorSteps::Scatter, timer.lap());

  const auto output_column_definitions =
      build_output_column_definitions(input_table, _groupby_column_ids, aggregate_schema, _aggregates);

  auto per_worker_outputs =
      run_merge_phase(key_schema, aggregate_schema, layout, scatter_stores, output_column_definitions, partition_count,
                      shift, key_width, cardinality_estimate, worker_limit, aggregate_count);
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
  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  auto timer = Timer{};

  const auto aggregators = build_aggregators(input_table, _aggregates);

  const auto morsel_count = static_cast<size_t>(input_table.chunk_count());
  const auto worker_limit = worker_limit_for(Hyrise::get().is_multi_threaded(), Hyrise::get().topology.num_cpus());
  const auto worker_count = std::clamp(morsel_count, size_t{1}, worker_limit);

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
      const auto chunk_id = ChunkID{static_cast<ChunkID::base_type>(morsel)};
      const auto chunk = input_table.get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }
      for (const auto& aggregator : aggregators) {
        aggregator->accumulate(worker_id, chunk_id, *chunk);
      }
    }
  });
  step_performance_data.set_step_runtime(OperatorSteps::Scatter, timer.lap());

  for (const auto& aggregator : aggregators) {
    aggregator->merge();
  }
  step_performance_data.set_step_runtime(OperatorSteps::Merge, timer.lap());

  _output_column_definitions.reserve(aggregators.size());
  _output_segments.reserve(aggregators.size());
  for (const auto& aggregator : aggregators) {
    _output_column_definitions.push_back(aggregator->output_column_definition());
    _output_segments.push_back(aggregator->build_segment());
  }

  auto output_table = std::make_shared<Table>(_output_column_definitions, TableType::Data);
  output_table->append_chunk(_output_segments);
  step_performance_data.set_step_runtime(OperatorSteps::OutputWriting, timer.lap());
  return output_table;
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
