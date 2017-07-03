#include "sort_node.hpp"

#include <string>

namespace opossum {

SortNode::SortNode(const std::string column_name, const bool asc)
    : AbstractNode(NodeType::Sort), _column_name(column_name), _asc(asc) {}

const std::string SortNode::description() const {
  return "Sort: " + _column_name + " (" + (_asc ? "asc" : "desc") + ")";
}

const std::string SortNode::column_name() const { return _column_name; };

const bool SortNode::asc() const { return _asc; };

}  // namespace opossum
