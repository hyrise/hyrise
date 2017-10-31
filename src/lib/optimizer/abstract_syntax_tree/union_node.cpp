#include "union_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

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

const std::vector<std::string>& UnionNode::output_column_names() const {
  DebugAssert(left_child()->output_column_names() == right_child()->output_column_names(), "Input layouts differ.");
  return left_child()->output_column_names();
}

const std::vector<ColumnID>& UnionNode::output_column_ids_to_input_column_ids() const {
  if (!_output_column_ids_to_input_column_ids) {
    _output_column_ids_to_input_column_ids->resize(output_column_count());
    std::iota(_output_column_ids_to_input_column_ids->begin(), _output_column_ids_to_input_column_ids->end(),
              ColumnID{0});
  }
  return *_output_column_ids_to_input_column_ids;
}

std::shared_ptr<TableStatistics> UnionNode::derive_statistics_from(
    const std::shared_ptr<AbstractASTNode>& left_child, const std::shared_ptr<AbstractASTNode>& right_child) const {
  Fail("Statistics for UNION not yet implemented");
  return nullptr;  // Return something
}

std::optional<ColumnID> UnionNode::find_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(left_child() && !right_child(),
              "Node has no or two inputs and therefore needs to override this function");

  auto named_column_reference_without_local_alias = _resolve_local_alias(named_column_reference);
  if (!named_column_reference_without_local_alias) {
    return {};
  } else {
    const auto column_id_in_left =
        left_child()->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);
#if IS_DEBUG
    const auto column_id_in_right =
        right_child()->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);
    DebugAssert(column_id_in_left == column_id_in_right, "Input layouts don't match");
#endif
    return column_id_in_left;
  }
}

bool UnionNode::knows_table(const std::string& table_name) const {
  return false;  // knows_table() is not a concept that applies to Unions
}

std::vector<ColumnID> UnionNode::get_output_column_ids_for_table(const std::string& table_name) const {
  Fail("get_output_column_ids_for_table() is not a concept that applies to Unions");
  return {};
}

}  // namespace opossum
