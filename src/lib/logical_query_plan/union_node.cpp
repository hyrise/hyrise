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
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  return UnionNode::make(_union_mode);
}

UnionMode UnionNode::union_mode() const { return _union_mode; }

std::string UnionNode::description() const { return "[UnionNode] Mode: " + union_mode_to_string.at(_union_mode); }

std::string UnionNode::get_verbose_column_name(ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need inputs to determine Column name");
  Assert(column_id < left_input()->output_column_names().size(), "ColumnID out of range");
  Assert(right_input()->output_column_names().size() == left_input()->output_column_names().size(),
         "Input node mismatch");

  const auto left_column_name = left_input()->output_column_names()[column_id];

  const auto right_column_name = right_input()->output_column_names()[column_id];
  Assert(left_column_name == right_column_name, "Input column names don't match");

  if (_table_alias) {
    return *_table_alias + "." + left_column_name;
  }

  return left_column_name;
}

const std::vector<std::string>& UnionNode::output_column_names() const {
  DebugAssert(left_input()->output_column_names() == right_input()->output_column_names(), "Input layouts differ.");
  return left_input()->output_column_names();
}

const std::vector<LQPColumnReference>& UnionNode::output_column_references() const {
  if (!_output_column_references) {
    DebugAssert(left_input()->output_column_references() == right_input()->output_column_references(),
                "Input layouts differ.");
    _output_column_references = left_input()->output_column_references();
  }
  return *_output_column_references;
}

std::shared_ptr<TableStatistics> UnionNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  Fail("Statistics for UNION not yet implemented");
}

bool UnionNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& union_node = static_cast<const UnionNode&>(rhs);

  return _union_mode == union_node._union_mode;
}

}  // namespace opossum
