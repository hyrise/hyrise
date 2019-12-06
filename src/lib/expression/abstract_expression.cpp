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

bool AbstractExpression::is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  const auto node_column_id = lqp.find_column_id(*this);
  if (node_column_id) {
    return lqp.is_column_nullable(*node_column_id);
  }

  return _on_is_nullable_on_lqp(lqp);
}

bool AbstractExpression::operator==(const AbstractExpression& other) const {
  if (type != other.type) return false;
  if (!expressions_equal(arguments, other.arguments)) return false;
  return _shallow_equals(other);
}

bool AbstractExpression::operator!=(const AbstractExpression& other) const { return !operator==(other); }

size_t AbstractExpression::hash() const {
  auto hash = boost::hash_value(type);
  for (const auto& argument : arguments) {
    boost::hash_combine(hash, argument->hash());
  }
  boost::hash_combine(hash, _shallow_hash());
  return hash;
}

std::string AbstractExpression::as_column_name() const { return description(DescriptionMode::ColumnName); }

size_t AbstractExpression::_shallow_hash() const { return 0; }

bool AbstractExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  return std::any_of(arguments.begin(), arguments.end(),
                     [&](const auto& expression) { return expression->is_nullable_on_lqp(lqp); });
}

ExpressionPrecedence AbstractExpression::_precedence() const { return ExpressionPrecedence::Highest; }

std::string AbstractExpression::_enclose_argument(const AbstractExpression& argument,
                                                  const DescriptionMode mode) const {
  // TODO(anybody) Using >= to make divisions ("(2/3)/4") and logical operations ("(a AND (b OR c))") unambiguous -
  //               Sadly this makes cases where the parentheses could be avoided look ugly ("(2+3)+4")

  if (static_cast<std::underlying_type_t<ExpressionPrecedence>>(argument._precedence()) >=
      static_cast<std::underlying_type_t<ExpressionPrecedence>>(_precedence())) {
    return "("s + argument.description(mode) + ")";
  } else {
    return argument.description(mode);
  }
}

}  // namespace opossum
