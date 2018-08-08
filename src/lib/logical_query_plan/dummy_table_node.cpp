#include "dummy_table_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "expression/value_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

DummyTableNode::DummyTableNode() : AbstractLQPNode(LQPNodeType::DummyTable) {}

std::string DummyTableNode::description() const { return "[DummyTable]"; }

const std::vector<std::shared_ptr<AbstractExpression>>& DummyTableNode::column_expressions() const {
  return _column_expressions;
}

std::shared_ptr<AbstractLQPNode> DummyTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<DummyTableNode>();
}

bool DummyTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace opossum
