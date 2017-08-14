#include "sort_node.hpp"

#include <string>
#include <boost/lexical_cast.hpp>

#include "types.hpp"

namespace opossum {

SortNode::SortNode(const ColumnID &column_id, const bool asc)
    : AbstractASTNode(ASTNodeType::Sort), _column_id(column_id), _ascending(asc) {}

std::string SortNode::description() const {
  return "Sort: " + boost::lexical_cast<std::string>(_column_id) + " (" + (_ascending ? "asc" : "desc") + ")";
}

ColumnID SortNode::column_id() const { return _column_id; }

bool SortNode::ascending() const { return _ascending; }

}  // namespace opossum
