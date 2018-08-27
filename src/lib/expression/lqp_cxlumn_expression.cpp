#include "lqp_cxlumn_expression.hpp"

#include "boost/functional/hash.hpp"

#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPCxlumnExpression::LQPCxlumnExpression(const LQPCxlumnReference& cxlumn_reference)
    : AbstractExpression(ExpressionType::LQPColumn, {}), cxlumn_reference(cxlumn_reference) {}

std::shared_ptr<AbstractExpression> LQPCxlumnExpression::deep_copy() const {
  return std::make_shared<LQPCxlumnExpression>(cxlumn_reference);
}

std::string LQPCxlumnExpression::as_cxlumn_name() const {
  Assert(cxlumn_reference.original_node(), "Node referenced by LQPCxlumnReference has expired");

  if (cxlumn_reference.original_node()->type == LQPNodeType::StoredTable) {
    std::stringstream stream;
    stream << cxlumn_reference;
    return stream.str();

  } else if (cxlumn_reference.original_node()->type == LQPNodeType::Mock) {
    const auto mock_node = std::static_pointer_cast<const MockNode>(cxlumn_reference.original_node());
    Assert(cxlumn_reference.original_cxlumn_id() < mock_node->cxlumn_definitions().size(), "CxlumnID out of range");
    return mock_node->cxlumn_definitions()[cxlumn_reference.original_cxlumn_id()].second;

  } else {
    Fail("Only columns in StoredTableNodes and MockNodes (for tests) can be referenced in LQPCxlumnExpressions");
  }
}

DataType LQPCxlumnExpression::data_type() const {
  if (cxlumn_reference.original_node()->type == LQPNodeType::StoredTable) {
    const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(cxlumn_reference.original_node());
    const auto table = StorageManager::get().get_table(stored_table_node->table_name);
    return table->cxlumn_data_type(cxlumn_reference.original_cxlumn_id());

  } else if (cxlumn_reference.original_node()->type == LQPNodeType::Mock) {
    const auto mock_node = std::static_pointer_cast<const MockNode>(cxlumn_reference.original_node());
    Assert(cxlumn_reference.original_cxlumn_id() < mock_node->cxlumn_definitions().size(), "CxlumnID out of range");
    return mock_node->cxlumn_definitions()[cxlumn_reference.original_cxlumn_id()].first;

  } else {
    Fail("Only columns in StoredTableNodes and MockNodes (for tests) can be referenced in LQPCxlumnExpressions");
  }
}

bool LQPCxlumnExpression::is_nullable() const {
  if (cxlumn_reference.original_node()->type == LQPNodeType::StoredTable) {
    const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(cxlumn_reference.original_node());
    const auto table = StorageManager::get().get_table(stored_table_node->table_name);
    return table->column_is_nullable(cxlumn_reference.original_cxlumn_id());

  } else if (cxlumn_reference.original_node()->type == LQPNodeType::Mock) {
    return false;  // MockNodes do not support NULLs

  } else {
    Fail("Only columns in StoredTableNodes and MockNodes (for tests) can be referenced in LQPCxlumnExpressions");
  }
}

bool LQPCxlumnExpression::requires_computation() const { return false; }

bool LQPCxlumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto* lqp_column_expression = dynamic_cast<const LQPCxlumnExpression*>(&expression);
  Assert(lqp_column_expression, "Expected LQPCxlumnExpression");
  return cxlumn_reference == lqp_column_expression->cxlumn_reference;
}

size_t LQPCxlumnExpression::_on_hash() const { return std::hash<LQPCxlumnReference>{}(cxlumn_reference); }

}  // namespace opossum
