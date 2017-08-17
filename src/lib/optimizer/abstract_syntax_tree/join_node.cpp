#include "join_node.hpp"

#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common.hpp"
#include "constant_mappings.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinNode::JoinNode(optional<std::pair<ColumnID, ColumnID>> join_column_ids, const ScanType scan_type,
                   const JoinMode join_mode)
    : AbstractASTNode(ASTNodeType::Join),
      _join_column_ids(join_column_ids),
      _scan_type(scan_type),
      _join_mode(join_mode) {}

std::string JoinNode::description() const {
  std::ostringstream desc;

  desc << "Join";
  desc << " [" << join_mode_to_string.at(_join_mode) << "]";

  if (_join_column_ids) {
    desc << " [" << (*_join_column_ids).first;
    desc << " " << scan_type_to_string.left.at(_scan_type);
    desc << " " << (*_join_column_ids).second << "]";
  }

  return desc.str();
}

std::vector<ColumnID> JoinNode::output_column_ids() const {
  /**
   * Collect the output ColumnIDs of the children on the fly, because the children might change.
   */
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");

  std::vector<ColumnID> output_column_ids;

  for (auto column_id : left_child()->output_column_ids()) {
    output_column_ids.emplace_back(column_id);
  }

  // Incorporate the offset of the columns of the left table.
  auto offset_column_id = left_child()->output_column_ids().size();
  for (auto column_id : right_child()->output_column_ids()) {
    output_column_ids.emplace_back(offset_column_id + column_id);
  }

  return output_column_ids;
}

std::vector<std::string> JoinNode::output_column_names() const {
  /**
   * Collect the output column names of the children on the fly, because the children might change.
   */
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");

  const auto &left_names = left_child()->output_column_names();
  const auto &right_names = right_child()->output_column_names();

  std::vector<std::string> output_column_names;
  output_column_names.reserve(left_names.size() + right_names.size());

  output_column_names.insert(output_column_names.end(), left_names.begin(), left_names.end());
  output_column_names.insert(output_column_names.end(), right_names.begin(), right_names.end());

  return output_column_names;
}

optional<ColumnID> JoinNode::find_column_id_for_column_identifier(const ColumnIdentifier &column_identifier) const {
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");

  // If there is no qualifying table name, search both children.
  if (!column_identifier.table_name) {
    const auto &left_column_id = left_child()->find_column_id_for_column_identifier(column_identifier);
    const auto &right_column_id = right_child()->find_column_id_for_column_identifier(column_identifier);

    Assert(!left_column_id || !right_column_id, "Column name " + column_identifier.column_name + " is ambiguous.");

    if (left_column_id) {
      return left_column_id;
    }

    // Optional might not be set.
    return right_column_id;
  }

  // There must not be two tables with the same qualifying name.
  Assert(!left_child()->manages_table(*column_identifier.table_name) ||
             !right_child()->manages_table(*column_identifier.table_name),
         "Table name " + *column_identifier.table_name + " is ambiguous.");

  // Otherwise only search a children if it manages that qualifier.
  if (left_child()->manages_table(*column_identifier.table_name)) {
    return left_child()->find_column_id_for_column_identifier(column_identifier);
  }

  if (right_child()->manages_table(*column_identifier.table_name)) {
    auto column_id = right_child()->find_column_id_for_column_identifier(column_identifier);
    if (column_id) {
      auto column_idx = left_child()->output_column_ids().size() + (*column_id);
      DebugAssert(column_idx < std::numeric_limits<ColumnID>::max(), "Too many columns for table.");
      return ColumnID{static_cast<uint16_t>(column_idx)};
    }
  }

  return {};
}

bool JoinNode::manages_table(const std::string &table_name) const {
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");
  return left_child()->manages_table(table_name) || right_child()->manages_table(table_name);
}

optional<std::pair<ColumnID, ColumnID>> JoinNode::join_column_ids() const { return _join_column_ids; }

ScanType JoinNode::scan_type() const { return _scan_type; }

JoinMode JoinNode::join_mode() const { return _join_mode; }

}  // namespace opossum
