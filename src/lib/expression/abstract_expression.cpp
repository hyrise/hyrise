#include "abstract_expression.hpp"

#include <queue>
#include <string>

#include "boost/functional/hash.hpp"
#include "expression_utils.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

AbstractExpression::AbstractExpression(const ExpressionType type,
                                       const std::vector<std::shared_ptr<AbstractExpression>>& arguments)
    : type(type), arguments(arguments) {}

bool AbstractExpression::requires_computation() const { return true; }

bool AbstractExpression::is_nullable() const {
  return std::any_of(arguments.begin(), arguments.end(),
                     [](const auto& expression) { return expression->is_nullable(); });
}

bool AbstractExpression::operator==(const AbstractExpression& other) const {
  if (type != other.type) return false;
  if (!expressions_equal(arguments, other.arguments)) return false;
  return _shallow_equals(other);
}

bool AbstractExpression::operator!=(const AbstractExpression& other) const { return !operator==(other); }

size_t AbstractExpression::hash() const {
  auto hash = boost::hash_value(static_cast<ExpressionType>(type));
  for (const auto& argument : arguments) {
    boost::hash_combine(hash, argument->hash());
  }
  boost::hash_combine(hash, _on_hash());
  return hash;
}

size_t AbstractExpression::_on_hash() const { return 0; }

ExpressionPrecedence AbstractExpression::_precedence() const { return ExpressionPrecedence::Highest; }

std::string AbstractExpression::_enclose_argument_as_column_name(const AbstractExpression& argument) const {
  // TODO(anybody) Using >= to make divisions ("(2/3)/4") and logical operations ("(a AND (b OR c))") unambiguous -
  //               Sadly this makes cases where the parentheses could be avoided look ugly ("(2+3)+4")

  if (static_cast<std::underlying_type_t<ExpressionPrecedence>>(argument._precedence()) >=
      static_cast<std::underlying_type_t<ExpressionPrecedence>>(_precedence())) {
    return "("s + argument.as_column_name() + ")";
  } else {
    return argument.as_column_name();
  }
}

}  // namespace opossum
