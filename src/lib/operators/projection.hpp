#pragma once

#include "abstract_read_only_operator.hpp"  // NEEDEDINCLUDE

namespace opossum {
class AbstractExpression;

/**
 * Operator to evaluate Expressions (except for AggregateExpressions)
 */
class Projection : public AbstractReadOnlyOperator {
 public:
  Projection(const std::shared_ptr<const AbstractOperator>& in,
             const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  const std::string name() const override;

  static std::shared_ptr<Table> dummy_table();

  const std::vector<std::shared_ptr<AbstractExpression>> expressions;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
};

}  // namespace opossum
