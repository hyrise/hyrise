#include "lqp_column_expression.hpp"

#include "boost/functional/hash.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPColumnExpression::LQPColumnExpression(const std::shared_ptr<const AbstractLQPNode>& init_original_node,
                                         const ColumnID init_original_column_id)
    : AbstractExpression(ExpressionType::LQPColumn, {}),
      original_node(init_original_node),
      original_column_id(init_original_column_id) {}

std::shared_ptr<AbstractExpression> LQPColumnExpression::deep_copy() const {
  return std::make_shared<LQPColumnExpression>(original_node.lock(), original_column_id);
}

std::string LQPColumnExpression::description(const DescriptionMode mode) const {
  // Even if the LQP is invalid, we still want to be able to print it as good as possible
  const auto original_node_locked = original_node.lock();
  if (!original_node_locked) return "<Expired Column>";

  std::stringstream output;
  if (mode == AbstractExpression::DescriptionMode::Detailed) {
    output << original_node_locked << ".";
  }

  if (original_column_id == INVALID_COLUMN_ID) {
    output << "INVALID_COLUMN_ID";
    return output.str();
  }

  switch (original_node_locked->type) {
    case LQPNodeType::StoredTable: {
      const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(original_node_locked);
      const auto table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      output << table->column_name(original_column_id);
      return output.str();
    }

    case LQPNodeType::Mock: {
      const auto mock_node = std::static_pointer_cast<const MockNode>(original_node_locked);
      Assert(original_column_id < mock_node->column_definitions().size(), "ColumnID out of range");
      output << mock_node->column_definitions()[original_column_id].second;
      return output.str();
    }

    case LQPNodeType::StaticTable: {
      const auto static_table_node = std::static_pointer_cast<const StaticTableNode>(original_node_locked);
      output << static_table_node->table->column_name(original_column_id);
      return output.str();
    }

    default: {
      Fail("Node type can not be referenced in LQPColumnExpressions");
    }
  }
}

DataType LQPColumnExpression::data_type() const {
  const auto original_node_locked = original_node.lock();
  Assert(original_node_locked, "Trying to retrieve data_type of expired LQPColumnExpression, LQP is invalid");

  if (original_column_id == INVALID_COLUMN_ID) {
    // Handle COUNT(*). Note: This is the input data type.
    return DataType::Long;
  }

  switch (original_node_locked->type) {
    case LQPNodeType::StoredTable: {
      const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(original_node_locked);
      const auto table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      return table->column_data_type(original_column_id);
    }

    case LQPNodeType::Mock: {
      const auto mock_node = std::static_pointer_cast<const MockNode>(original_node_locked);
      Assert(original_column_id < mock_node->column_definitions().size(), "ColumnID out of range");
      return mock_node->column_definitions()[original_column_id].first;
    }

    case LQPNodeType::StaticTable: {
      const auto static_table_node = std::static_pointer_cast<const StaticTableNode>(original_node_locked);
      return static_table_node->table->column_data_type(original_column_id);
    }

    default: {
      Fail("Node type can not be referenced in LQPColumnExpressions");
    }
  }
}

bool LQPColumnExpression::requires_computation() const { return false; }

bool LQPColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const LQPColumnExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& lqp_column_expression = static_cast<const LQPColumnExpression&>(expression);
  return original_column_id == lqp_column_expression.original_column_id &&
         original_node.lock() == lqp_column_expression.original_node.lock();
}

size_t LQPColumnExpression::_shallow_hash() const {
  // It is important not to combine the address of the original_node with the hash code as it was done before #1795.
  // If this address is combined with the return hash code, equal LQP nodes that are not identical and that have
  // LQPColumnExpressions or child nodes with LQPColumnExpressions would have different hash codes.
  auto hash = boost::hash_value(original_node.lock()->hash());
  boost::hash_combine(hash, static_cast<size_t>(original_column_id));
  return hash;
}

bool LQPColumnExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  Fail(
      "Should not be called. This should have been forwarded to StoredTableNode/StaticTableNode/MockNode by "
      "AbstractExpression::is_nullable_on_lqp()");
}

}  // namespace opossum
