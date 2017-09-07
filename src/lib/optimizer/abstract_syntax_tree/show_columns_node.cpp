#include "show_columns_node.hpp"

#include <string>

namespace opossum {

ShowColumnsNode::ShowColumnsNode(const std::string &table_name)
    : AbstractNonOptimizableASTNode(ASTNodeType::ShowColumns), _table_name(table_name) {}

std::string ShowColumnsNode::description() const { return "ShowColumns: " + _table_name; }

const std::string &ShowColumnsNode::table_name() const { return _table_name; }

}  // namespace opossum
