#include "union_node.hpp"

#include <memory>
#include <string>

#include "constant_mappings.hpp"
#include "utils/assert.hpp"

namespace opossum {

UnionNode::UnionNode(UnionMode union_mode) : AbstractASTNode(ASTNodeType::Union), _union_mode(union_mode) {}

UnionMode UnionNode::union_mode() const { return _union_mode; }

std::string UnionNode::description() const { return "[UnionNode] Mode: " + union_mode_to_string.at(_union_mode); }

std::string UnionNode::get_verbose_column_name(ColumnID column_id) const {
  Assert(left_child() && right_child(), "Need children to determine Column name");

  const auto left_column_name = left_child()->get_verbose_column_name(column_id);
  const auto right_column_name = right_child()->get_verbose_column_name(column_id);

  Assert(left_column_name == right_column_name, "Input column names don't match");

  return left_column_name;
}

std::shared_ptr<AbstractASTNode> UnionNode::clone_subtree() const {
  return _clone_without_subclass_members<decltype(*this)>();
}

}  // namespace opossum
