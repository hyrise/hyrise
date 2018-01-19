#include "union_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "utils/assert.hpp"

namespace opossum {

UnionNode::UnionNode(UnionMode union_mode) : AbstractLQPNode(LQPNodeType::Union), _union_mode(union_mode) {}

std::shared_ptr<AbstractLQPNode> UnionNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<UnionNode>(_union_mode);
}

UnionMode UnionNode::union_mode() const { return _union_mode; }

std::string UnionNode::description() const { return "[UnionNode] Mode: " + union_mode_to_string.at(_union_mode); }

std::string UnionNode::get_verbose_column_name(ColumnID column_id) const {
  Assert(left_child() && right_child(), "Need children to determine Column name");
  Assert(column_id < left_child()->output_column_names().size(), "ColumnID out of range");
  Assert(right_child()->output_column_names().size() == left_child()->output_column_names().size(),
         "Input node mismatch");

  const auto left_column_name = left_child()->output_column_names()[column_id];

  const auto right_column_name = right_child()->output_column_names()[column_id];
  Assert(left_column_name == right_column_name, "Input column names don't match");

  if (_table_alias) {
    return *_table_alias + "." + left_column_name;
  }

  return left_column_name;
}

const std::vector<std::string>& UnionNode::output_column_names() const {
  DebugAssert(left_child()->output_column_names() == right_child()->output_column_names(), "Input layouts differ.");
  return left_child()->output_column_names();
}

const std::vector<LQPColumnReference>& UnionNode::output_column_references() const {
  if (!_output_column_references) {
    DebugAssert(left_child()->output_column_references() == right_child()->output_column_references(),
                "Input layouts differ.");
    _output_column_references = left_child()->output_column_references();
  }
  return *_output_column_references;
}

std::shared_ptr<TableStatistics> UnionNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const {
  Fail("Statistics for UNION not yet implemented");
}
}  // namespace opossum
