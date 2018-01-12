#include "dummy_table_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

DummyTableNode::DummyTableNode() : AbstractLQPNode(LQPNodeType::DummyTable) { _output_column_references.emplace(); }

std::shared_ptr<AbstractLQPNode> DummyTableNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<DummyTableNode>();
}

std::string DummyTableNode::description() const { return "[DummyTable]"; }

const std::vector<std::string>& DummyTableNode::output_column_names() const { return _output_column_names; }

}  // namespace opossum
