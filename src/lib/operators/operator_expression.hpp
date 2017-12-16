#pragma once

#include <optional>

#include "expression.hpp"
#include "types.hpp"

namespace opossum {

class LQPExpression;

class OperatorExpression : public Expression {
 public:
  static std::shared_ptr<OperatorExpression> create_column(const ColumnID column_id,
                                                                        const std::optional<std::string>& alias);

  using Expression::Expression;
  OperatorExpression(const std::shared_ptr<LQPExpression>& lqp_expression, const std::shared_ptr<AbstractLQPNode>& node);

  ColumnID column_id() const;

  std::string to_string(const std::optional<std::vector<std::string>>& input_column_names) const override;

 private:
  std::optional<ColumnID> _column_id;
};

}