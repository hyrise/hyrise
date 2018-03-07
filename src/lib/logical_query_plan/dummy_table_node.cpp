#include "dummy_table_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

DummyTableNode::DummyTableNode() : AbstractLQPNode(LQPNodeType::DummyTable) { _output_column_references.emplace(); }

std::shared_ptr<AbstractLQPNode> DummyTableNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  return DummyTableNode::make();
}

std::string DummyTableNode::description() const { return "[DummyTable]"; }

const std::vector<std::string>& DummyTableNode::output_column_names() const { return _output_column_names; }

bool DummyTableNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  return true;
}

}  // namespace opossum
