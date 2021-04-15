#include "placeholder_expression.hpp"

#include <sstream>
#include <string>
#include <type_traits>

#include <boost/container_hash/hash.hpp>

#include "resolve_type.hpp"

namespace opossum {

PlaceholderExpression::PlaceholderExpression(const ParameterID init_parameter_id)
    : AbstractExpression(ExpressionType::Placeholder, {}), parameter_id(init_parameter_id) {}

std::shared_ptr<AbstractExpression> PlaceholderExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<PlaceholderExpression>(parameter_id);
}

std::string PlaceholderExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "Placeholder[ParameterID=" << std::to_string(parameter_id) << "]";
  return stream.str();
}

bool PlaceholderExpression::requires_computation() const { return false; }

DataType PlaceholderExpression::data_type() const { Fail("Cannot obtain DataType of placeholder"); }

bool PlaceholderExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const PlaceholderExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& parameter_expression_rhs = static_cast<const PlaceholderExpression&>(expression);
  return parameter_id == parameter_expression_rhs.parameter_id;
}

size_t PlaceholderExpression::_shallow_hash() const {
  return boost::hash_value(static_cast<ParameterID::base_type>(parameter_id));
}

bool PlaceholderExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  // Placeholder COULD be replaced with NULL
  return true;
}

}  // namespace opossum
