#include "between_expression.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_precedence.hpp"
#include "operators/abstract_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

BetweenExpression::BetweenExpression(const PredicateCondition init_predicate_condition,
                                     const std::shared_ptr<AbstractExpression>& operand,
                                     const std::shared_ptr<AbstractExpression>& lower_bound,
                                     const std::shared_ptr<AbstractExpression>& upper_bound)
    : AbstractPredicateExpression(init_predicate_condition, {operand, lower_bound, upper_bound}) {
  Assert(is_between_predicate_condition(predicate_condition),
         "Predicate Condition not supported by Between Expression");
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::operand() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::lower_bound() const {
  return arguments[1];
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::upper_bound() const {
  return arguments[2];
}

std::shared_ptr<AbstractExpression> BetweenExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<BetweenExpression>(predicate_condition, operand()->deep_copy(copied_ops),
                                             lower_bound()->deep_copy(copied_ops),
                                             upper_bound()->deep_copy(copied_ops));
}

std::string BetweenExpression::description(const DescriptionMode mode) const {
  auto stream = std::stringstream{};
  stream << _enclose_argument(*operand(), mode) << " " << predicate_condition << " "
         << _enclose_argument(*lower_bound(), mode) << " AND " << _enclose_argument(*upper_bound(), mode);
  return stream.str();
}

ExpressionPrecedence BetweenExpression::_precedence() const {
  return ExpressionPrecedence::BinaryTernaryPredicate;
}

}  // namespace hyrise
