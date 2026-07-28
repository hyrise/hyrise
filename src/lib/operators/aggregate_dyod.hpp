#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_aggregate_operator.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "types.hpp"

namespace hyrise {

class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  std::shared_ptr<const Table> no_groupby_aggregate();
  std::shared_ptr<const Table> groupby_aggregate();

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;
};

}  // namespace hyrise
