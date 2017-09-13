#include "dummy_table_node.hpp"

#include <string>

#include "utils/assert.hpp"

namespace opossum {

DummyTableNode::DummyTableNode() : AbstractASTNode(ASTNodeType::DummyTable) {}

std::string DummyTableNode::description() const { return "DummyTable"; }

void DummyTableNode::_on_child_changed() { Fail("DummyTableNode cannot have children."); }

}  // namespace opossum
