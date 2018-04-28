#include "pqp_column_expression.hpp"

#include "boost/functional/hash.hpp"

namespace opossum {

PQPColumnExpression::PQPColumnExpression(const ColumnID column_id, const DataType data_type, const bool nullable): column_id(column_id), _data_type(data_type), _nullable(nullable) {}

std::shared_ptr<AbstractExpression> PQPColumnExpression::deep_copy() const {
  return std::make_shared<PQPColumnExpression>(column_id, _data_type, _nullable);
}

std::string PQPColumnExpression::as_column_name() const {
  Fail("TODO");
}

DataType PQPColumnExpression::data_type() const {
  return _data_type;
}

bool PQPColumnExpression::is_nullable() const {
  return _nullable;
}

bool PQPColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  return column_id == static_cast<const PQPColumnExpression&>(expression).column_id;
}

size_t PQPColumnExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(column_id));
}

}  // namespace opossum
