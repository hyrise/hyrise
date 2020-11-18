#include "placeholder_expression.hpp"

#include <sstream>
#include <string>
#include <type_traits>

#include "boost/functional/hash.hpp"

#include "resolve_type.hpp"

namespace opossum {

PlaceholderExpression::PlaceholderExpression(const ParameterID init_parameter_id)
    : AbstractExpression(ExpressionType::Placeholder, {}), parameter_id(init_parameter_id), optional_data_type(std::nullopt) {}

PlaceholderExpression::PlaceholderExpression(const ParameterID init_parameter_id, const DataType init_data_type)
    : AbstractExpression(ExpressionType::Placeholder, {}), parameter_id(init_parameter_id), optional_data_type(init_data_type) {
      /* Placeholders with a data_type are used for cache parameterization, where null values should not be replaced by
      * placeholers. */
      DebugAssert(*optional_data_type != DataType::Null, "Typed Placeholder should not be DataType::Null");
    }

std::shared_ptr<AbstractExpression> PlaceholderExpression::deep_copy() const {
  if (optional_data_type) {
    return std::make_shared<PlaceholderExpression>(parameter_id, *optional_data_type);  
  } else {
    return std::make_shared<PlaceholderExpression>(parameter_id);  
  }
}

std::string PlaceholderExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "Placeholder[id=" << std::to_string(parameter_id) << "]";
  return stream.str();
}

bool PlaceholderExpression::requires_computation() const { return false; }

DataType PlaceholderExpression::data_type() const { 
  if (optional_data_type) {
    return *optional_data_type; 
  } else {
    Fail("Cannot obtain DataType of non-typed placeholder");
  }
}

bool PlaceholderExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const PlaceholderExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& parameter_expression_rhs = static_cast<const PlaceholderExpression&>(expression);
  return (parameter_id == parameter_expression_rhs.parameter_id) && (optional_data_type == parameter_expression_rhs.optional_data_type);
}

size_t PlaceholderExpression::_shallow_hash() const {
  auto hash = boost::hash_value(static_cast<ParameterID::base_type>(parameter_id));
  boost::hash_combine(hash, optional_data_type);
  return hash;
}

bool PlaceholderExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  // Placeholder COULD be replaced with NULL
  // Typed placeholder (with a data_type assigned) should not be replaced with NULL
  return !optional_data_type.has_value();
}

}  // namespace opossum
