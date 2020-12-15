#include "value_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"
#include "resolve_type.hpp"

namespace opossum {

ValueExpression::ValueExpression(const AllTypeVariant& init_value,
                                 const std::optional<ValueExpressionID> init_value_expression_id)
    : AbstractExpression(ExpressionType::Value, {}), value(init_value), value_expression_id(init_value_expression_id) {}

bool ValueExpression::requires_computation() const { return false; }

std::shared_ptr<AbstractExpression> ValueExpression::deep_copy() const {
  if (value_expression_id) {
    return std::make_shared<ValueExpression>(value, *value_expression_id);
  } else {
    return std::make_shared<ValueExpression>(value);
  }
}

std::string ValueExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;

  if (value.type() == typeid(pmr_string)) {
    stream << "'" << value << "'";
  } else {
    stream << value;
  }

  if (value.type() == typeid(int64_t)) {
    stream << "L";
  } else if (value.type() == typeid(float)) {
    stream << "F";
  }

  return stream.str();
}

DataType ValueExpression::data_type() const { return data_type_from_all_type_variant(value); }

bool ValueExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ValueExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& value_expression = static_cast<const ValueExpression&>(expression);

  /**
   * Even though null != null, two null expressions are *the same expressions* (e.g. when resolving ColumnIDs)
   */
  if (data_type() == DataType::Null && value_expression.data_type() == DataType::Null) return true;

  const auto value_equality = (value == value_expression.value);
  // Checking for value id equality would be reasonable, however SQLite QUery 285 doesn't like that
  // SELECT b + 1, COUNT(c + 1) FROM mixed GROUP BY b+1
  // const auto value_id_equality = (value_expression_id && value_expression.value_expression_id)
  //                                     ? (*value_expression_id == *value_expression.value_expression_id)
  //                                     : true;
  // return value_equality && value_id_equality;
  return value_equality;
}

size_t ValueExpression::_shallow_hash() const { return std::hash<AllTypeVariant>{}(value); }

bool ValueExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  return value.type() == typeid(NullValue);
}

}  // namespace opossum
