#include "pqp_column_expression.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

std::shared_ptr<PQPColumnExpression> PQPColumnExpression::from_table(const Table& table,
                                                                     const std::string& column_name) {
  const auto column_id = table.column_id_by_name(column_name);
  return std::make_shared<PQPColumnExpression>(column_id, table.column_data_type(column_id),
                                               table.column_is_nullable(column_id), column_name);
}

std::shared_ptr<PQPColumnExpression> PQPColumnExpression::from_table(const Table& table, const ColumnID column_id) {
  return PQPColumnExpression::from_table(table, table.column_name(column_id));
}

PQPColumnExpression::PQPColumnExpression(const ColumnID init_column_id, const DataType data_type, const bool nullable,
                                         const std::string& column_name)
    : AbstractExpression(ExpressionType::PQPColumn, {}),
      column_id(init_column_id),
      _data_type(data_type),
      _nullable(nullable),
      _column_name(column_name) {}

std::shared_ptr<AbstractExpression> PQPColumnExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<PQPColumnExpression>(column_id, _data_type, _nullable, _column_name);
}

std::string PQPColumnExpression::description(const DescriptionMode /*mode*/) const {
  return _column_name;
}

DataType PQPColumnExpression::data_type() const {
  return _data_type;
}

bool PQPColumnExpression::requires_computation() const {
  return false;
}

bool PQPColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const PQPColumnExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& pqp_column_expression = static_cast<const PQPColumnExpression&>(expression);
  return column_id == pqp_column_expression.column_id && _data_type == pqp_column_expression._data_type &&
         _nullable == pqp_column_expression._nullable && _column_name == pqp_column_expression._column_name;
}

size_t PQPColumnExpression::_shallow_hash() const {
  return std::hash<ColumnID::base_type>{}(column_id);
}

bool PQPColumnExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const {
  Fail("Nullability 'on lqp' should never be queried from a PQPColumn");
}

}  // namespace hyrise
