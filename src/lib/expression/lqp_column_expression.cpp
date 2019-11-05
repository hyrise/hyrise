#include "lqp_column_expression.hpp"

#include "boost/functional/hash.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPColumnExpression::LQPColumnExpression(const LQPColumnReference& column_reference)
    : AbstractExpression(ExpressionType::LQPColumn, {}), column_reference(column_reference) {}

std::shared_ptr<AbstractExpression> LQPColumnExpression::deep_copy() const {
  return std::make_shared<LQPColumnExpression>(column_reference);
}

std::string LQPColumnExpression::as_column_name() const {
  // Even if the LQP is invalid, we still want to be able to print it as good as possible
  const auto original_node = column_reference.original_node();
  if (!original_node) return "<Expired Column>";

  if (original_node->type == LQPNodeType::StoredTable) {
    std::stringstream stream;
    stream << column_reference;
    return stream.str();

  } else if (original_node->type == LQPNodeType::Mock) {
    const auto mock_node = std::static_pointer_cast<const MockNode>(original_node);
    Assert(column_reference.original_column_id() < mock_node->column_definitions().size(), "ColumnID out of range");
    return mock_node->column_definitions()[column_reference.original_column_id()].second;

  } else if (column_reference.original_node()->type == LQPNodeType::StaticTable) {
    const auto static_table_node = std::static_pointer_cast<const StaticTableNode>(column_reference.original_node());
    return static_table_node->table->column_name(column_reference.original_column_id());
  } else {
    Fail("Node type can not be referenced in LQPColumnExpressions");
  }
}

DataType LQPColumnExpression::data_type() const {
  const auto original_node = column_reference.original_node();
  if (column_reference.original_column_id() == INVALID_COLUMN_ID) {
    // Handle COUNT(*). Note: This is the input data type.
    return DataType::Long;
  } else if (column_reference.original_node()->type == LQPNodeType::StoredTable) {
    const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(column_reference.original_node());
    const auto table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
    return table->column_data_type(column_reference.original_column_id());

  } else if (original_node->type == LQPNodeType::Mock) {
    const auto mock_node = std::static_pointer_cast<const MockNode>(original_node);
    Assert(column_reference.original_column_id() < mock_node->column_definitions().size(), "ColumnID out of range");
    return mock_node->column_definitions()[column_reference.original_column_id()].first;

  } else if (column_reference.original_node()->type == LQPNodeType::StaticTable) {
    const auto static_table_node = std::static_pointer_cast<const StaticTableNode>(column_reference.original_node());
    return static_table_node->table->column_data_type(column_reference.original_column_id());
  } else {
    Fail("Node type can not be referenced in LQPColumnExpressions");
  }
}

bool LQPColumnExpression::requires_computation() const { return false; }

bool LQPColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const LQPColumnExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& lqp_column_expression = static_cast<const LQPColumnExpression&>(expression);
  return column_reference == lqp_column_expression.column_reference;
}

size_t LQPColumnExpression::_shallow_hash() const { return std::hash<LQPColumnReference>{}(column_reference); }

bool LQPColumnExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  Fail(
      "Should not be called. This should have been forwarded to StoredTableNode/StaticTableNode/MockNode by "
      "AbstractExpression::is_nullable_on_lqp()");
}

}  // namespace opossum
