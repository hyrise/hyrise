#include "sort_node.hpp"

#include <string>

namespace opossum {

SortNode::SortNode(const std::string column_name, const bool asc)
    : AbstractAstNode(AstNodeType::Sort), _column_name(column_name), _ascending(asc) {}

std::string SortNode::description() const {
    return "Sort: " + _column_name + " (" + (_ascending ? "asc" : "desc") + ")";
}

std::string SortNode::column_name() const { return _column_name; }

bool SortNode::ascending() const { return _ascending; }

}  // namespace opossum
