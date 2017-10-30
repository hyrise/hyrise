#include "show_columns_node.hpp"

#include <memory>
#include <string>

namespace opossum {

ShowColumnsNode::ShowColumnsNode(const std::string& table_name)
    : AbstractNonOptimizableASTNode(ASTNodeType::ShowColumns), _table_name(table_name) {}

std::string ShowColumnsNode::description() const { return "[ShowColumns] Table: '" + _table_name + "'"; }

const std::string& ShowColumnsNode::table_name() const { return _table_name; }

std::shared_ptr<AbstractASTNode> ShowColumnsNode::clone_subtree() const {
  return _clone_without_subclass_members<decltype(*this)>();
}

}  // namespace opossum
