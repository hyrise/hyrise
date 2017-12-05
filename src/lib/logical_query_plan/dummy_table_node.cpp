#include "dummy_table_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

DummyTableNode::DummyTableNode() : AbstractLQPNode(LQPNodeType::DummyTable) {
  _output_column_ids_to_input_column_ids.emplace();
}

std::shared_ptr<AbstractLQPNode> DummyTableNode::_deep_copy_impl() const { return std::make_shared<DummyTableNode>(); }

std::string DummyTableNode::description() const { return "[DummyTable]"; }

const std::vector<std::string>& DummyTableNode::output_column_names() const { return _output_column_names; }

}  // namespace opossum
