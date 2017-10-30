#include "show_tables_node.hpp"

#include <memory>
#include <string>

namespace opossum {

ShowTablesNode::ShowTablesNode() : AbstractNonOptimizableASTNode(ASTNodeType::ShowTables) {}

std::string ShowTablesNode::description() const { return "[ShowTables]"; }

std::shared_ptr<AbstractASTNode> ShowTablesNode::clone_subtree() const {
  return _clone_without_subclass_members<decltype(*this)>();
}

}  // namespace opossum
