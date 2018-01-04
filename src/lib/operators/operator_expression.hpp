#pragma once

#include <optional>

#include "base_expression.hpp"
#include "types.hpp"

namespace opossum {

class LQPExpression;

/**
 * Expression type used in PQPs, using ColumnIDs to refer to Columns
 */
class OperatorExpression : public BaseExpression<OperatorExpression> {
 public:
  static std::shared_ptr<OperatorExpression> create_column(const ColumnID column_id,
                                                           const std::optional<std::string>& alias = std::nullopt);

  using BaseExpression<OperatorExpression>::BaseExpression;

  /**
   * Translates a LQPExpression into a OperatorExpression, given the node that the LQPExpression is contained in
   */
  OperatorExpression(const std::shared_ptr<LQPExpression>& lqp_expression,
                     const std::shared_ptr<AbstractLQPNode>& node);

  ColumnID column_id() const;

  std::string to_string(const std::optional<std::vector<std::string>>& input_column_names = std::nullopt,
                        bool is_root = true) const override;

  bool operator==(const OperatorExpression& other) const;

 protected:
  void _deep_copy_impl(const std::shared_ptr<OperatorExpression>& copy) const override;

 private:
  std::optional<ColumnID> _column_id;
};
}  // namespace opossum
