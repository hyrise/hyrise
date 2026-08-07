#pragma once

#include <algorithm>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>
#include <oneapi/tbb/concurrent_unordered_map.h>  // NOLINT(build/include_order)

#include "abstract_aggregate_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "aggregate/window_function_traits.hpp"
#include "expression/window_function_expression.hpp"
#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

using GroupID = size_t;
using GroupKeyEntry = std::vector<std::byte>;
using GroupKey = std::vector<GroupKeyEntry>;
using GroupIDMap = tbb::concurrent_unordered_map<GroupKey, GroupID, boost::hash<GroupKey>>;

class AbstractAggregateVector {
 public:
  virtual ~AbstractAggregateVector() = default;
  virtual void grow_if_necessary(size_t size) = 0;
  virtual void merge(AbstractAggregateVector& other) = 0;

  size_t count(GroupID group_id) {
    return _counts[group_id];
  }

  void occupy(GroupID group_id) {
    _occupied[group_id] = true;
    _group_count++;
  }

  void increment_count(GroupID group_id) {
    _counts[group_id]++;
  }

  pmr_vector<size_t> counts() {
    // clang-format off
    auto view = std::views::iota(size_t{0}, _counts.size())
      | std::views::filter([&](size_t index) { return _occupied[index]; })
      | std::views::transform([&](size_t index) { return _counts[index]; });
    // clang-format on

    return pmr_vector<size_t>(view.begin(), view.end());
  }

  GroupID group_count() const {
    return _group_count;
  }

 protected:
  size_t _group_count;
  std::vector<size_t> _counts;
  std::vector<bool> _occupied;
};

template <typename ColumnDataType, WindowFunction aggregate_function>
struct TypedAggregateVector : AbstractAggregateVector {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  using DistinctValues = std::unordered_set<ColumnDataType>;
  using AccumulatorDataType =
      std::conditional_t<aggregate_function == WindowFunction::CountDistinct, DistinctValues, AggregateDataType>;

 public:
  // The mutable accessor is needed because aggregator functions mutate values directly.
  AccumulatorDataType& accumulator(GroupID group_id) {
    return _accumulators[group_id];
  }

  pmr_vector<AccumulatorDataType> accumulators() {
    // clang-format off
    auto view = std::views::iota(size_t{0}, _accumulators.size())
      | std::views::filter([&](size_t index) { return _occupied[index]; })
      | std::views::transform([&](size_t index) { return _accumulators[index]; });
    // clang-format on

    return pmr_vector<AccumulatorDataType>(view.begin(), view.end());
  }

  void grow_if_necessary(size_t size) override {
    if (_counts.size() < size) {
      _counts.resize(size);
      _accumulators.resize(size);
      _occupied.resize(size);
    }
  }

  void merge(AbstractAggregateVector& other) override {
    auto& typed_other = static_cast<TypedAggregateVector<ColumnDataType, aggregate_function>&>(other);
    _merge(typed_other);
  }

 protected:
  pmr_vector<AccumulatorDataType> _accumulators;

  void _merge(TypedAggregateVector<ColumnDataType, aggregate_function>& other) {
    const auto new_size = std::max(_accumulators.size(), other._accumulators.size());

    _accumulators.resize(new_size);
    _counts.resize(new_size);
    _occupied.resize(new_size);

    if constexpr (aggregate_function == WindowFunction::CountDistinct) {
      auto& other_accumulators = other._accumulators;
      const auto other_size = other_accumulators.size();

      for (auto index = size_t{0}; index < new_size; ++index) {
        if (index < other_size) {
          _accumulators[index].merge(other_accumulators[index]);
        }
      }
    } else if constexpr (aggregate_function == WindowFunction::Count) {
      const auto& other_counts = other._counts;
      const auto other_size = other_counts.size();

      for (auto index = size_t{0}; index < new_size; ++index) {
        if (index < other_size && other_counts[index] > 0) {
          _counts[index] += other_counts[index];
        }
      }
    } else if constexpr (aggregate_function == WindowFunction::Any) {
      const auto& other_accumulators = other._accumulators;
      const auto& other_counts = other._counts;
      const auto other_size = other_accumulators.size();

      for (auto index = size_t{0}; index < new_size; ++index) {
        if (_counts[index] == 0 && index < other_size && other_counts[index] > 0) {
          _accumulators[index] = other_accumulators[index];
          _counts[index] += other_counts[index];
        }
      }
    } else {
      const auto& other_accumulators = other._accumulators;
      const auto& other_counts = other._counts;
      const auto other_size = other_accumulators.size();

      auto aggregator =
          WindowFunctionBuilder<AggregateDataType, AggregateDataType, aggregate_function>().get_aggregate_function();

      for (auto index = size_t{0}; index < new_size; ++index) {
        if (index < other_size && other_counts[index] > 0) {
          aggregator(other_accumulators[index], _counts[index], _accumulators[index]);
          _counts[index] += other_counts[index];
        }
      }
    }

    const auto other_occupied = other._occupied;

    for (auto index = size_t{0}; index < new_size; ++index) {
      if (index < other._occupied.size() && !_occupied[index] && other_occupied[index]) {
        _occupied[index] = true;
        _group_count++;
      }
    }
  }
};

class WorkerState : public Noncopyable {
 public:
  explicit WorkerState(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                       std::function<std::pair<GroupID, GroupID>()> init_group_id_range);
  void merge(WorkerState& other);

  GroupID next_group_id();

  AbstractAggregateVector& aggregate_vector(size_t index);
  std::vector<std::unique_ptr<AbstractAggregateVector>>& aggregate_vectors();

 protected:
  // This is the next local group ID that a worker can assign to a group key
  GroupID _next_group_id;

  // This is the largest local group ID that a worker can assign to a group key
  GroupID _max_group_id;

  std::function<std::pair<GroupID, GroupID>()> _get_new_group_id_range;
  std::vector<std::unique_ptr<AbstractAggregateVector>> _vectors;
};

template <typename T>
  requires std::is_trivially_copyable_v<T>
std::vector<std::byte> serialize_value(T value);

std::vector<std::byte> serialize_value(const pmr_string& value);

template <typename T>
  requires std::is_trivially_copyable_v<T>
std::vector<std::byte> serialize_value(T value, bool is_null);

std::vector<std::byte> serialize_value(const pmr_string& value, bool is_null);

template <typename T, bool Nullable>
  requires std::is_trivially_copyable_v<T> && (!Nullable)
T deserialize_value(const std::vector<std::byte>& bytes);

template <typename T, bool Nullable>
  requires std::is_trivially_copyable_v<T> && Nullable
std::optional<T> deserialize_value(const std::vector<std::byte>& bytes);

template <typename T, bool Nullable>
  requires std::is_same_v<T, pmr_string> && (!Nullable)
pmr_string deserialize_value(const std::vector<std::byte>& bytes);

template <typename T, bool Nullable>
  requires std::is_same_v<T, pmr_string> && Nullable
std::optional<pmr_string> deserialize_value(const std::vector<std::byte>& bytes);

template <typename Functor>
void resolve_window_function(WindowFunction window_function, Functor&& functor);

class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

  std::pair<GroupID, GroupID> _get_new_group_id_range();

 protected:
  // The paper uses a default step size of 256
  // https://github.com/danielxue/global-hash-tables-strike-back/blob/main/common/src/fuzzy_counter.rs#L56
  static constexpr GroupID FUZZY_STEP_SIZE = 128;

  std::vector<DataType> _aggregate_data_types;
  GroupIDMap _group_id_map;
  std::mutex _group_id_map_mutex;
  std::atomic<GroupID> _next_group_id;
  tbb::concurrent_vector<GroupKey> _group_keys;
  tbb::concurrent_vector<bool> _occupied_group_keys;
  std::mutex _group_keys_mutex;

  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  void _prepare_aggregate_vectors();

  std::shared_ptr<Table> _write_output_table(WorkerState& worker_state);

  template <typename ColumnDataType>
  std::shared_ptr<AbstractSegment> _write_groupby_segment(size_t groupby_column_index, std::vector<GroupKey>&,
                                                          size_t start_group_index, size_t end_group_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
      size_t start_group_index, size_t end_group_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_avg_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, size_t start_group_index,
      size_t end_group_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_count_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, size_t start_group_index,
      size_t end_group_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_count_distinct_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, size_t start_group_index,
      size_t end_group_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_default_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
      size_t start_group_index, size_t end_group_index);

  GroupID _group_id(const GroupKey& group_key, WorkerState& worker_state);

  std::vector<GroupID> _group_ids_for_chunk(const Chunk& chunk, WorkerState& worker_state);

  void _aggregate_chunk(WorkerState& state, const std::shared_ptr<const Chunk> chunk);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  void _aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                          const AbstractSegment& segment, const std::vector<GroupID>& group_ids);

  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::CountDistinct)
  void _aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                          const AbstractSegment& segment, const std::vector<GroupID>& group_ids);

  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::Any)
  void _aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                          const AbstractSegment& segment, const std::vector<GroupID>& group_ids);

  void _aggregate_count_star(AbstractAggregateVector& state, const std::vector<GroupID>& group_ids);

  DataType _aggregate_data_type(size_t aggregate_index);

  std::string _aggregate_column_name(size_t aggregate_index);

  bool _aggregate_is_nullable(size_t aggregate_index);

  DataType _aggregate_column_data_type(size_t aggregate_index);
};

}  // namespace hyrise
