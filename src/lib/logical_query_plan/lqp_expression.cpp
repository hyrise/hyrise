#include "lqp_expression.hpp"

#include "column_origin.hpp"
#include "constant_mappings.hpp"
#include "operators/operator_expression.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<LQPExpression> LQPExpression::create_column(const ColumnOrigin& column_origin,
                                                            const std::optional<std::string>& alias) {
  auto expression = std::make_shared<LQPExpression>(ExpressionType::Column);
  expression->_column_origin = column_origin;
  expression->_alias = alias;

  return expression;
}

std::vector<std::shared_ptr<LQPExpression>> LQPExpression::create_columns(
    const std::vector<ColumnOrigin>& column_origins, const std::optional<std::vector<std::string>>& aliases) {
  std::vector<std::shared_ptr<LQPExpression>> column_expressions;
  column_expressions.reserve(column_origins.size());

  if (!aliases) {
    for (const auto& column_origin : column_origins) {
      column_expressions.emplace_back(create_column(column_origin));
    }
  } else {
    DebugAssert(column_origins.size() == (*aliases).size(), "There must be the same number of aliases as ColumnIDs");

    for (auto column_index = 0u; column_index < column_origins.size(); ++column_index) {
      column_expressions.emplace_back(create_column(column_origins[column_index], (*aliases)[column_index]));
    }
  }

  return column_expressions;
}

const ColumnOrigin& LQPExpression::column_origin() const {
  DebugAssert(_column_origin, "Expression " + expression_type_to_string.at(_type) + " does not have a ColumnOrigin");
  return *_column_origin;
}

std::string LQPExpression::to_string(const std::optional<std::vector<std::string>>& input_column_names,
                                     bool is_root) const {
  if (type() == ExpressionType::Column) {
    return column_origin().get_verbose_name();
  }
  return Expression<LQPExpression>::to_string(input_column_names, is_root);
}

bool LQPExpression::operator==(const LQPExpression& other) const {
  if (!Expression<LQPExpression>::operator==(other)) {
    return false;
  }
  return _column_origin == other._column_origin;
}

void LQPExpression::_deep_copy_impl(const std::shared_ptr<LQPExpression> &copy) const {
  copy->_column_origin = _column_origin;
}

}