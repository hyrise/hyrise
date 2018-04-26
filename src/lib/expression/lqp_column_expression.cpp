#include "lqp_column_expression.hpp"

#include "boost/functional/hash.hpp"

#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPColumnExpression::LQPColumnExpression(const LQPColumnReference& column_reference):
  column_reference(column_reference) {}

std::shared_ptr<AbstractExpression> LQPColumnExpression::deep_copy() const {
  return std::make_shared<LQPColumnExpression>(column_reference);
}

std::string LQPColumnExpression::as_column_name() const {
  Fail("TODO");
}

ExpressionDataTypeVariant LQPColumnExpression::data_type() const {
  const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(column_reference.original_node());
  Assert(stored_table_node, "Expected column reference to point to StoredTableNode");

  const auto table = StorageManager::get().get_table(stored_table_node->table_name);
  return table->column_data_type(column_reference.original_column_id());
}

bool LQPColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& lqp_column_expression = static_cast<const LQPColumnExpression&>(expression);
  return column_reference == lqp_column_expression.column_reference;
}

size_t LQPColumnExpression::_on_hash() const {
  return std::hash<LQPColumnReference>{}(column_reference);
}

}  // namespace opossum
