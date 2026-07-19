#include "aggregate_dyod.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <limits>
#include <memory>
#include <memory_resource>
#include <numeric>
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
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

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

    segment_iterate<ColumnDataType>(*chunk.get_segment(_column_id), [&](const auto& position) {
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

std::unique_ptr<AbstractAggregator> make_aggregator(const Table& input_table, const WindowFunctionExpression& aggregate,
                                                    const ColumnID column_id) {
  const auto window_function = aggregate.window_function;
  auto output_name = aggregate.as_column_name();

  if (window_function == WindowFunction::Count && column_id == INVALID_COLUMN_ID) {
    return std::make_unique<CountStarAggregator>(std::move(output_name));
  }
  Assert(column_id != INVALID_COLUMN_ID, "Only COUNT(*) can have an invalid ColumnID.");

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
}  // namespace

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  Assert(_groupby_column_ids.empty(), "AggregateDYOD currently supports only queries without GROUP BY.");
  _validate_aggregates();

  const auto input_table = left_input_table();
  const auto aggregators = build_aggregators(*input_table, _aggregates);

  // TODO: currently morsel_count == chunk_count, this has to be adjusted
  const auto morsel_count = static_cast<size_t>(input_table->chunk_count());
  const auto worker_count = std::clamp(morsel_count, size_t{1}, Hyrise::get().topology.num_cpus());

  for (const auto& aggregator : aggregators) {
    aggregator->set_worker_count(worker_count);
  }

  auto morsel_cursor = std::atomic<size_t>{0};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(worker_count);

  for (auto worker_id = size_t{0}; worker_id < worker_count; ++worker_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, worker_id]() {
      while (true) {
        const auto morsel = morsel_cursor.fetch_add(1, std::memory_order_relaxed);
        if (morsel >= morsel_count) {
          break;
        }
        const auto chunk = input_table->get_chunk(ChunkID{static_cast<ChunkID::base_type>(morsel)});
        if (!chunk) {
          continue;
        }
        for (const auto& aggregator : aggregators) {
          aggregator->accumulate(worker_id, *chunk);
        }
      }
    }));
  }

  if (jobs.size() > 1) {
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  } else if (!jobs.empty()) {
    jobs.front()->execute();
  }

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
