#include "pqp_column_expression.hpp"

#include "boost/functional/hash.hpp"

namespace opossum {

PQPColumnExpression::PQPColumnExpression(const ColumnID column_id): column_id(column_id) {}

std::shared_ptr<AbstractExpression> PQPColumnExpression::deep_copy() const {
  return std::make_shared<PQPColumnExpression>(column_id);
}

std::string PQPColumnExpression::as_column_name() const {
  Fail("TODO");
}

ExpressionDataTypeVariant PQPColumnExpression::data_type() const {
  Fail("Can't determine the data_type of a PQPColumnExpression, you need to do that yourself");
}

bool PQPColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  return column_id == static_cast<const PQPColumnExpression&>(expression).column_id;
}

size_t PQPColumnExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(column_id));
}

}  // namespace opossum
