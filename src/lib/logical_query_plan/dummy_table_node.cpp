#include "dummy_table_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/order_dependency.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

DummyTableNode::DummyTableNode() : AbstractLQPNode(LQPNodeType::DummyTable) {}

std::string DummyTableNode::description(const DescriptionMode /*mode*/) const {
  return "[DummyTable]";
}

std::vector<std::shared_ptr<AbstractExpression>> DummyTableNode::output_expressions() const {
  return {};
}

bool DummyTableNode::is_column_nullable(const ColumnID /*column_id*/) const {
  Fail("DummyTable does not output any columns");
}

UniqueColumnCombinations DummyTableNode::unique_column_combinations() const {
  return UniqueColumnCombinations{};
}

OrderDependencies DummyTableNode::order_dependencies() const {
  return OrderDependencies{};
}

std::shared_ptr<AbstractLQPNode> DummyTableNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return std::make_shared<DummyTableNode>();
}

bool DummyTableNode::_on_shallow_equals(const AbstractLQPNode& /*rhs*/, const LQPNodeMapping& /*node_mapping*/) const {
  return true;
}

}  // namespace hyrise
