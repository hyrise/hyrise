#include "pqp_cxlumn_expression.hpp"

#include "boost/functional/hash.hpp"

#include "storage/table.hpp"

namespace opossum {

std::shared_ptr<PQPCxlumnExpression> PQPCxlumnExpression::from_table(const Table& table,
                                                                     const std::string& cxlumn_name) {
  const auto cxlumn_id = table.cxlumn_id_by_name(cxlumn_name);
  return std::make_shared<PQPCxlumnExpression>(cxlumn_id, table.cxlumn_data_type(cxlumn_id),
                                               table.column_is_nullable(cxlumn_id), cxlumn_name);
}

PQPCxlumnExpression::PQPCxlumnExpression(const CxlumnID cxlumn_id, const DataType data_type, const bool nullable,
                                         const std::string& cxlumn_name)
    : AbstractExpression(ExpressionType::PQPColumn, {}),
      cxlumn_id(cxlumn_id),
      _data_type(data_type),
      _nullable(nullable),
      _cxlumn_name(cxlumn_name) {}

std::shared_ptr<AbstractExpression> PQPCxlumnExpression::deep_copy() const {
  return std::make_shared<PQPCxlumnExpression>(cxlumn_id, _data_type, _nullable, _cxlumn_name);
}

std::string PQPCxlumnExpression::as_cxlumn_name() const { return _cxlumn_name; }

DataType PQPCxlumnExpression::data_type() const { return _data_type; }

bool PQPCxlumnExpression::is_nullable() const { return _nullable; }

bool PQPCxlumnExpression::requires_computation() const { return false; }

bool PQPCxlumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto* pqp_column_expression = dynamic_cast<const PQPCxlumnExpression*>(&expression);
  if (!pqp_column_expression) return false;
  return cxlumn_id == pqp_column_expression->cxlumn_id;
}

size_t PQPCxlumnExpression::_on_hash() const { return boost::hash_value(static_cast<size_t>(cxlumn_id)); }

}  // namespace opossum
