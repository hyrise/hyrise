#include "placeholder_expression.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

PlaceholderExpression::PlaceholderExpression(const ParameterID init_parameter_id)
    : AbstractExpression(ExpressionType::Placeholder, {}), parameter_id(init_parameter_id) {}

std::shared_ptr<AbstractExpression> PlaceholderExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<PlaceholderExpression>(parameter_id);
}

std::string PlaceholderExpression::description(const DescriptionMode /*mode*/) const {
  auto stream = std::stringstream{};
  stream << "Placeholder[ParameterID=" << std::to_string(parameter_id) << "]";
  return stream.str();
}

bool PlaceholderExpression::requires_computation() const {
  return false;
}

DataType PlaceholderExpression::data_type() const {
  Fail("Cannot obtain DataType of placeholder");
}

bool PlaceholderExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const PlaceholderExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& parameter_expression_rhs = static_cast<const PlaceholderExpression&>(expression);
  return parameter_id == parameter_expression_rhs.parameter_id;
}

size_t PlaceholderExpression::_shallow_hash() const {
  return std::hash<ParameterID::base_type>{}(static_cast<ParameterID::base_type>(parameter_id));
}

bool PlaceholderExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const {
  // Placeholder COULD be replaced with NULL
  return true;
}

}  // namespace hyrise
