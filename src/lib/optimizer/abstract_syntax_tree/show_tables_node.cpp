#include "show_tables_node.hpp"

#include <string>

namespace opossum {

ShowTablesNode::ShowTablesNode() : AbstractNonOptimizableASTNode(ASTNodeType::ShowTables) {}

std::string ShowTablesNode::description() const { return "ShowTables"; }

}  // namespace opossum
