#include "lqp_column_expression.hpp"

#include "boost/functional/hash.hpp"

#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/mock_node.hpp"
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
  if (column_reference.original_node()->type == LQPNodeType::StoredTable) {
    const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(column_reference.original_node());
    const auto table = StorageManager::get().get_table(stored_table_node->table_name);
    return table->column_name(column_reference.original_column_id());

  } else if (column_reference.original_node()->type == LQPNodeType::Mock) {
    const auto mock_node = std::static_pointer_cast<const MockNode>(column_reference.original_node());
    Assert(column_reference.original_column_id() < mock_node->column_definitions().size(), "ColumnID out of range");
    return mock_node->column_definitions()[column_reference.original_column_id()].second;

  } else {
    Fail("Not yet implemented");
  }
}

DataType LQPColumnExpression::data_type() const {
  if (column_reference.original_node()->type == LQPNodeType::StoredTable) {
    const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(column_reference.original_node());
    const auto table = StorageManager::get().get_table(stored_table_node->table_name);
    return table->column_data_type(column_reference.original_column_id());

  } else if (column_reference.original_node()->type == LQPNodeType::Mock) {
    const auto mock_node = std::static_pointer_cast<const MockNode>(column_reference.original_node());
    Assert(column_reference.original_column_id() < mock_node->column_definitions().size(), "ColumnID out of range");
    return mock_node->column_definitions()[column_reference.original_column_id()].first;

  } else {
    Fail("Not yet implemented");
  }
}

bool LQPColumnExpression::is_nullable() const {
  if (column_reference.original_node()->type == LQPNodeType::StoredTable) {
    const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(column_reference.original_node());
    const auto table = StorageManager::get().get_table(stored_table_node->table_name);
    return table->column_is_nullable(column_reference.original_column_id());

  } else if (column_reference.original_node()->type == LQPNodeType::Mock) {
    return false; // MockNodes do not support NULLs

  } else {
    Fail("Not yet implemented");
  }  
}

bool LQPColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto* lqp_column_expression = dynamic_cast<const LQPColumnExpression*>(&expression);
  Assert(lqp_column_expression, "Expected LQPColumnExpression");
  return column_reference == lqp_column_expression->column_reference;
}

size_t LQPColumnExpression::_on_hash() const {
  return std::hash<LQPColumnReference>{}(column_reference);
}

}  // namespace opossum
