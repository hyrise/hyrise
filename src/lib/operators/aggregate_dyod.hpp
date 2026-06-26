#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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

template <typename T>
  requires std::is_trivially_copyable_v<T>
std::vector<std::byte> serialize_value(T value);

std::vector<std::byte> serialize_value(const pmr_string& value);

template <typename T>
  requires std::is_trivially_copyable_v<T>
std::vector<std::byte> serialize_value(T value, bool is_null);

std::vector<std::byte> serialize_value(const pmr_string& value, bool is_null);

class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

 protected:
  std::vector<DataType> _aggregate_data_types;

  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  void _resolve_aggregate_data_types();

  std::shared_ptr<Table> _create_output_table();

  void _aggregate_chunk(std::shared_ptr<Chunk> chunk);
};

}  // namespace hyrise
