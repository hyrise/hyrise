#include "arithmetic_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "expression_utils.hpp"

namespace opossum {

std::ostream& operator<<(std::ostream& stream, const ArithmeticOperator arithmetic_operator) {
  switch (arithmetic_operator) {
    case ArithmeticOperator::Addition:
      stream << "+";
      break;
    case ArithmeticOperator::Subtraction:
      stream << "-";
      break;
    case ArithmeticOperator::Multiplication:
      stream << "*";
      break;
    case ArithmeticOperator::Division:
      stream << "/";
      break;
    case ArithmeticOperator::Modulo:
      stream << "%";
      break;
  }
  return stream;
}

ArithmeticExpression::ArithmeticExpression(const ArithmeticOperator init_arithmetic_operator,
                                           const std::shared_ptr<AbstractExpression>& left_operand,
                                           const std::shared_ptr<AbstractExpression>& right_operand)
    : AbstractExpression(ExpressionType::Arithmetic, {left_operand, right_operand}),
      arithmetic_operator(init_arithmetic_operator) {}

const std::shared_ptr<AbstractExpression>& ArithmeticExpression::left_operand() const { return arguments[0]; }

const std::shared_ptr<AbstractExpression>& ArithmeticExpression::right_operand() const { return arguments[1]; }

std::shared_ptr<AbstractExpression> ArithmeticExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<ArithmeticExpression>(arithmetic_operator, left_operand()->deep_copy(copied_ops),
                                                right_operand()->deep_copy(copied_ops));
}

DataType ArithmeticExpression::data_type() const {
  return expression_common_type(left_operand()->data_type(), right_operand()->data_type());
}

std::string ArithmeticExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;

  stream << _enclose_argument(*left_operand(), mode) << " " << arithmetic_operator << " "
         << _enclose_argument(*right_operand(), mode);

  return stream.str();
}

bool ArithmeticExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ArithmeticExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return arithmetic_operator == static_cast<const ArithmeticExpression&>(expression).arithmetic_operator;
}

size_t ArithmeticExpression::_shallow_hash() const {
  return boost::hash_value(static_cast<size_t>(arithmetic_operator));
}

bool ArithmeticExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  // We return NULL for divisions/modulo by 0
  return AbstractExpression::_on_is_nullable_on_lqp(lqp) || arithmetic_operator == ArithmeticOperator::Division ||
         arithmetic_operator == ArithmeticOperator::Modulo;
}

ExpressionPrecedence ArithmeticExpression::_precedence() const {
  switch (arithmetic_operator) {
    case ArithmeticOperator::Addition:
    case ArithmeticOperator::Subtraction:
      return ExpressionPrecedence::AdditionSubtraction;
    case ArithmeticOperator::Multiplication:
    case ArithmeticOperator::Division:
    case ArithmeticOperator::Modulo:
      return ExpressionPrecedence::MultiplicationDivision;
  }
  Fail("Invalid enum value");
}

}  // namespace opossum
