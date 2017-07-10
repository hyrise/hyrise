#include "sort_node.hpp"

#include <string>

namespace opossum {

SortNode::SortNode(const std::string column_name, const bool asc)
    : AbstractAstNode(AstNodeType::Sort), _column_name(column_name), _asc(asc) {}

std::string SortNode::description() const { return "Sort: " + _column_name + " (" + (_asc ? "asc" : "desc") + ")"; }

std::string SortNode::column_name() const { return _column_name; }

bool SortNode::asc() const { return _asc; }

}  // namespace opossum
