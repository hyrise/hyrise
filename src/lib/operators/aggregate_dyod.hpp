#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
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

using GroupKeyEntry = std::vector<std::byte>;
using GroupKey = std::vector<GroupKeyEntry>;
using Ticket = size_t;
using TicketTable = std::unordered_map<GroupKey, Ticket, boost::hash<GroupKey>>;

class AbstractAggregateVector {
 public:
  virtual ~AbstractAggregateVector() = default;
  virtual void push_back_default() = 0;

  size_t count(size_t index) const {
    return _counts[index];
  }

  void increment_count(size_t index) {
    _counts[index]++;
  }

  std::vector<size_t>& counts() {
    return _counts;
  }

  const std::vector<size_t>& counts() const {
    return _counts;
  }

  size_t size() {
    return _counts.size();
  }

 protected:
  std::vector<size_t> _counts;
};

template <typename AggregateDataType>
struct TypedAggregateVector : AbstractAggregateVector {
 public:
  AggregateDataType& operator[](size_t index) {
    return _aggregates[index];
  }

  const AggregateDataType& operator[](size_t index) const {
    return _aggregates[index];
  }

  pmr_vector<AggregateDataType>& values() {
    return _aggregates;
  }

  const pmr_vector<AggregateDataType>& values() const {
    return _aggregates;
  }

  void push_back_default() override {
    _aggregates.emplace_back();
    _counts.emplace_back();
  }

 protected:
  pmr_vector<AggregateDataType> _aggregates;
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

class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

 protected:
  std::vector<DataType> _aggregate_data_types;
  TicketTable _ticket_table = std::unordered_map<GroupKey, Ticket, boost::hash<GroupKey>>{};
  std::vector<GroupKey> _group_keys = std::vector<GroupKey>{};
  std::vector<std::unique_ptr<AbstractAggregateVector>> _aggregate_vectors;

  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  void _resolve_aggregate_data_types();

  std::shared_ptr<Table> _write_output_table();

  template <typename ColumnDataType>
  std::shared_ptr<AbstractSegment> _write_groupby_segment(size_t groupby_column_index);

  template <typename AggregateDataType>
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(size_t aggregate_index);

  Ticket _get_ticket(const GroupKey& group_key);

  void _aggregate_chunk(const std::shared_ptr<const Chunk> chunk);

  std::vector<Ticket> _get_tickets(const Chunk& chunk);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  void _aggregate_segment(size_t aggregate_index, const AbstractSegment& segment, const std::vector<Ticket>& tickets);

  DataType _aggregate_data_type(size_t aggregate_index);

  bool _aggregate_is_nullable(size_t aggregate_index);
};

}  // namespace hyrise
