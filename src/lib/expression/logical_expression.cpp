#include "logical_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"
#include "expression/evaluation/expression_evaluator.hpp"

namespace opossum {

std::ostream& operator<<(std::ostream& stream, const LogicalOperator logical_operator) {
  switch (logical_operator) {
    case LogicalOperator::And:
      stream << "AND";
      break;
    case LogicalOperator::Or:
      stream << "OR";
      break;
  }
  return stream;
}

LogicalExpression::LogicalExpression(const LogicalOperator logical_operator,
                                     const std::shared_ptr<AbstractExpression>& left_operand,
                                     const std::shared_ptr<AbstractExpression>& right_operand)
    : AbstractExpression(ExpressionType::Logical, {left_operand, right_operand}), logical_operator(logical_operator) {}

const std::shared_ptr<AbstractExpression>& LogicalExpression::left_operand() const { return arguments[0]; }

const std::shared_ptr<AbstractExpression>& LogicalExpression::right_operand() const { return arguments[1]; }

std::shared_ptr<AbstractExpression> LogicalExpression::deep_copy() const {
  return std::make_shared<LogicalExpression>(logical_operator, left_operand()->deep_copy(),
                                             right_operand()->deep_copy());
}

std::string LogicalExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << _enclose_argument(*left_operand(), mode) << " " << logical_operator << " "
         << _enclose_argument(*right_operand(), mode);
  return stream.str();
}

DataType LogicalExpression::data_type() const { return ExpressionEvaluator::DataTypeBool; }

bool LogicalExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const LogicalExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return logical_operator == static_cast<const LogicalExpression&>(expression).logical_operator;
}

size_t LogicalExpression::_shallow_hash() const { return boost::hash_value(static_cast<size_t>(logical_operator)); }

ExpressionPrecedence LogicalExpression::_precedence() const { return ExpressionPrecedence::Logical; }

}  // namespace opossum
