#pragma once

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

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
using GroupIDMap = std::unordered_map<GroupKey, GroupID, boost::hash<GroupKey>>;

class AbstractAggregateVector {
 public:
  virtual ~AbstractAggregateVector() = default;
  virtual void grow_if_necessary(size_t size) = 0;
  virtual void merge(AbstractAggregateVector& other) = 0;

  size_t count(size_t index) const {
    return _counts[index];
  }

  void increment_count(size_t index) {
    _counts[index]++;
  }

  const std::vector<size_t>& counts() const {
    return _counts;
  }

  GroupID group_count() const {
    return _counts.size();
  }

 protected:
  std::vector<size_t> _counts;
};

template <typename ColumnDataType, WindowFunction aggregate_function>
struct TypedAggregateVector : AbstractAggregateVector {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  using DistinctValues = std::unordered_set<ColumnDataType>;
  using AccumulatorDataType =
      std::conditional_t<aggregate_function == WindowFunction::CountDistinct, DistinctValues, AggregateDataType>;

 public:
  AccumulatorDataType& operator[](size_t index) {
    return _values[index];
  }

  const AccumulatorDataType& operator[](size_t index) const {
    return _values[index];
  }

  // The mutable accessor is needed because aggregator functions mutate values directly.
  std::vector<AccumulatorDataType>& values() {
    return _values;
  }

  const std::vector<AccumulatorDataType>& values() const {
    return _values;
  }

  void grow_if_necessary(size_t size) override {
    if (_counts.size() < size) {
      _counts.resize(size);
      _values.resize(size);
    }
  }

  void merge(AbstractAggregateVector& other) override {
    auto& typed_other = static_cast<TypedAggregateVector<ColumnDataType, aggregate_function>&>(other);
    _merge(typed_other);
  }

 protected:
  std::vector<AccumulatorDataType> _values;

  void _merge(TypedAggregateVector<ColumnDataType, aggregate_function>& other) {
    const auto other_group_count = other.group_count();
    const auto max_group_count = std::max(group_count(), other_group_count);

    _values.resize(max_group_count);
    _counts.resize(max_group_count);

    if constexpr (aggregate_function == WindowFunction::CountDistinct) {
      auto& other_values = other.values();

      for (auto group_id = GroupID{0}; group_id < max_group_count; ++group_id) {
        if (group_id < other_group_count) {
          _values[group_id].merge(other_values[group_id]);
        }
      }
    } else if constexpr (aggregate_function == WindowFunction::Count) {
      const auto& other_counts = other.counts();

      for (auto group_id = GroupID{0}; group_id < max_group_count; ++group_id) {
        if (group_id < other_group_count && other_counts[group_id] > 0) {
          _counts[group_id] += other_counts[group_id];
        }
      }
    } else if constexpr (aggregate_function == WindowFunction::Any) {
      const auto& other_values = other.values();
      const auto& other_counts = other.counts();

      for (auto group_id = GroupID{0}; group_id < max_group_count; ++group_id) {
        if (_counts[group_id] == 0 && group_id < other_group_count && other_counts[group_id] > 0) {
          _values[group_id] = other_values[group_id];
          _counts[group_id] += other_counts[group_id];
        }
      }
    } else {
      auto aggregator =
          WindowFunctionBuilder<AggregateDataType, AggregateDataType, aggregate_function>().get_aggregate_function();
      const auto& other_values = other.values();
      const auto& other_counts = other.counts();

      for (auto group_id = GroupID{0}; group_id < max_group_count; ++group_id) {
        if (group_id < other_group_count && other_counts[group_id] > 0) {
          aggregator(other_values[group_id], _counts[group_id], _values[group_id]);
          _counts[group_id] += other_counts[group_id];
        }
      }
    }
  }
};

class AggregateVectors : public Noncopyable {
 public:
  explicit AggregateVectors(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates);
  void merge(AggregateVectors& other);
  AbstractAggregateVector& operator[](size_t index);
  const AbstractAggregateVector& operator[](size_t index) const;
  auto begin();
  auto begin() const;
  auto end();
  auto end() const;

 protected:
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

 protected:
  std::vector<DataType> _aggregate_data_types;
  GroupIDMap _group_id_map;
  std::mutex _group_id_map_mutex;
  std::vector<GroupKey> _group_keys;

  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  void _prepare_aggregate_vectors();

  std::shared_ptr<Table> _write_output_table(AggregateVectors& aggregate_vectors);

  template <typename ColumnDataType>
  std::shared_ptr<AbstractSegment> _write_groupby_segment(size_t groupby_column_index, GroupID start_group_id,
                                                          GroupID end_group_id);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
      GroupID start_group_id, GroupID end_group_id);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_avg_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, GroupID start_group_id,
      GroupID end_group_id);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_count_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, GroupID start_group_id,
      GroupID end_group_id);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_count_distinct_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, GroupID start_group_id,
      GroupID end_group_id);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_default_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
      GroupID start_group_id, GroupID end_group_id);

  GroupID _group_id(const GroupKey& group_key, AggregateVectors& aggregate_vectors);

  GroupID _group_count();

  std::vector<GroupID> _group_ids_for_chunk(const Chunk& chunk, AggregateVectors& aggregate_vectors);

  void _aggregate_chunk(AggregateVectors& state, const std::shared_ptr<const Chunk> chunk);

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
