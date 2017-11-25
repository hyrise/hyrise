#include "show_tables_node.hpp"

#include <string>

namespace opossum {

ShowTablesNode::ShowTablesNode() : AbstractLQPNode(LQPNodeType::ShowTables) {}

std::string ShowTablesNode::description() const { return "[ShowTables]"; }

}  // namespace opossum
