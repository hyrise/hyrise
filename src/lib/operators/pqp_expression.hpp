#pragma once

#include <optional>

#include "abstract_expression.hpp"
#include "abstract_operator.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class LQPExpression;

/**
 * Expression type used in PQPs, using ColumnIDs to refer to Columns.
 * AbstractExpression handles all other possible contained types (literals, operators, ...).
 */
class PQPExpression : public AbstractExpression<PQPExpression> {
 public:
  static std::shared_ptr<PQPExpression> create_column(const ColumnID column_id,
                                                      const std::optional<std::string>& alias = std::nullopt);

  static std::shared_ptr<PQPExpression> create_subselect(std::shared_ptr<AbstractOperator> root_operator);

  // Necessary for the AbstractExpression<T>::create_*() methods
  using AbstractExpression<PQPExpression>::AbstractExpression;

  /**
   * Translates a LQPExpression into a PQPExpression, given the node that the LQPExpression is contained in
   */
  PQPExpression(const std::shared_ptr<LQPExpression>& lqp_expression, const std::shared_ptr<AbstractLQPNode>& node);

  ColumnID column_id() const;
  std::shared_ptr<AbstractOperator> subselect_operator();
  std::shared_ptr<const Table> table();

  void set_table(std::shared_ptr<const Table> table);

  std::string to_string(const std::optional<std::vector<std::string>>& input_column_names = std::nullopt,
                        bool is_root = true) const override;

  bool operator==(const PQPExpression& other) const;

 protected:
  void _deep_copy_impl(const std::shared_ptr<PQPExpression>& copy) const override;

 private:
  std::optional<ColumnID> _column_id;
  std::optional<std::shared_ptr<AbstractOperator>> _subselect_operator;
  std::optional<std::shared_ptr<const Table>> _table;
};
}  // namespace opossum
