#pragma once

#include <cstdint>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

/**
 * Operator to evaluate Expressions (except for AggregateExpressions)
 */
class Projection : public AbstractReadOnlyOperator {
 public:
  Projection(const std::shared_ptr<const AbstractOperator>& in,
             const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  const std::string name() const override;

  /**
   * The dummy table is used for literal projections that have no input table.
   * This was introduce to allow queries like INSERT INTO tbl VALUES (1, 2, 3);
   * Because each INSERT uses a projection as input, the above case needs to project the three
   * literals (1, 2, 3) without any specific input table. Therefore, this dummy table is used instead.
   *
   * The dummy table contains one (value) column with one row. This way, the above projection
   * contains exactly one row with the given literals.
   */
  class DummyTable : public Table {
   public:
    DummyTable() : Table(TableColumnDefinitions{{"dummy", DataType::Int}}, TableType::Data) {
      append(std::vector<AllTypeVariant>{0});
    }
  };

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
