#include "join_node.hpp"

#include <limits>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common.hpp"
#include "constant_mappings.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinNode::JoinNode(const JoinMode join_mode) : AbstractASTNode(ASTNodeType::Join), _join_mode(join_mode) {
  DebugAssert(join_mode == JoinMode::Cross || join_mode == JoinMode::Natural,
              "Specified JoinMode must also specify column ids and scan type.");
}

JoinNode::JoinNode(const JoinMode join_mode, const std::pair<ColumnID, ColumnID> &join_column_ids,
                   const ScanType scan_type)
    : AbstractASTNode(ASTNodeType::Join),
      _join_mode(join_mode),
      _join_column_ids(join_column_ids),
      _scan_type(scan_type) {
  DebugAssert(join_mode != JoinMode::Cross && join_mode != JoinMode::Natural,
              "Specified JoinMode must specify neither column ids nor scan type.");
}

std::string JoinNode::description() const {
  std::ostringstream desc;

  desc << "Join";
  desc << " [" << join_mode_to_string.at(_join_mode) << "]";

  if (_join_column_ids && _scan_type) {
    desc << " [" << (*_join_column_ids).first;
    desc << " " << scan_type_to_string.left.at(*_scan_type);
    desc << " " << (*_join_column_ids).second << "]";
  }

  return desc.str();
}

const std::vector<ColumnID> &JoinNode::output_column_id_to_input_column_id() const {
  return _output_column_id_to_input_column_id;
}

const std::vector<std::string> &JoinNode::output_column_names() const { return _output_column_names; }

optional<ColumnID> JoinNode::find_column_id_for_column_identifier_name(
    const ColumnIdentifierName &column_identifier_name) const {
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");

  optional<ColumnID> left_column_id;
  optional<ColumnID> right_column_id;

  // If there is no qualifying table name, search both children.
  if (!column_identifier_name.table_name) {
    left_column_id = left_child()->find_column_id_for_column_identifier_name(column_identifier_name);
    right_column_id = right_child()->find_column_id_for_column_identifier_name(column_identifier_name);
  } else {
    // Otherwise only search a children if it manages that qualifier.
    auto left_manages_table = left_child()->manages_table(*column_identifier_name.table_name);
    auto right_manages_table = right_child()->manages_table(*column_identifier_name.table_name);

    // If neither input table manages the table name, return.
    if (!left_manages_table && !right_manages_table) {
      return nullopt;
    }

    // There must not be two tables with the same qualifying name.
    Assert(left_manages_table ^ right_manages_table,
           "Table name " + *column_identifier_name.table_name + " is ambiguous.");

    if (left_manages_table) {
      left_column_id = left_child()->find_column_id_for_column_identifier_name(column_identifier_name);
    } else {
      right_column_id = right_child()->find_column_id_for_column_identifier_name(column_identifier_name);
    }
  }

  // If neither input table has that column, return.
  if (!left_column_id && !right_column_id) {
    return nullopt;
  }

  Assert(static_cast<bool>(left_column_id) ^ static_cast<bool>(right_column_id),
         "Column name " + column_identifier_name.column_name + " is ambiguous.");

  std::vector<ColumnID>::const_iterator iter_begin;
  std::vector<ColumnID>::const_iterator iter_end;
  ColumnID column_id;

  if (left_column_id) {
    column_id = *left_column_id;
    iter_begin = _output_column_id_to_input_column_id.begin();
    iter_end = _output_column_id_to_input_column_id.begin() + left_child()->num_output_columns();
  } else {
    column_id = *right_column_id;
    iter_begin = _output_column_id_to_input_column_id.begin() + left_child()->num_output_columns();
    iter_end = _output_column_id_to_input_column_id.end();
  }

  const auto iter = std::find(iter_begin, iter_end, column_id);

  // The join does not select columns from its input, but simply takes all columns from both children.
  DebugAssert(iter != _output_column_id_to_input_column_id.end(), "ColumnID should be in output.");

  // Return the offset of the ColumnID.
  const auto column_idx = std::distance(_output_column_id_to_input_column_id.begin(), iter);
  return ColumnID{static_cast<ColumnID::base_type>(column_idx)};
}

bool JoinNode::manages_table(const std::string &table_name) const {
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");
  return left_child()->manages_table(table_name) || right_child()->manages_table(table_name);
}

std::vector<ColumnID> JoinNode::get_output_column_ids_for_table(const std::string &table_name) const {
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");

  auto left_manages_table = left_child()->manages_table(table_name);
  auto right_manages_table = right_child()->manages_table(table_name);

  // If neither input table manages the table name, return.
  if (!left_manages_table && !right_manages_table) {
    return {};
  }

  // There must not be two tables with the same qualifying name.
  Assert(left_manages_table ^ right_manages_table, "Table name " + table_name + " is ambiguous.");

  if (left_manages_table) {
    // The ColumnIDs of the left table appear first in `_output_column_id_to_input_column_id`.
    // That means they are at the same position in our output, so we can return them directly.
    return left_child()->get_output_column_ids_for_table(table_name);
  }

  // The ColumnIDs of the right table appear after the ColumnIDs of the left table.
  // Add that offset to each of them and return that list.
  const auto input_column_ids_for_table = right_child()->get_output_column_ids_for_table(table_name);
  std::vector<ColumnID> output_column_ids_for_table;
  for (const auto input_column_id : input_column_ids_for_table) {
    const auto idx = left_child()->num_output_columns() + input_column_id;
    output_column_ids_for_table.emplace_back(static_cast<ColumnID::base_type>(idx));
  }

  return output_column_ids_for_table;
}

optional<std::pair<ColumnID, ColumnID>> JoinNode::join_column_ids() const { return _join_column_ids; }

optional<ScanType> JoinNode::scan_type() const { return _scan_type; }

JoinMode JoinNode::join_mode() const { return _join_mode; }

void JoinNode::_on_child_changed() {
  // Only set output information if both children have already been set.
  if (!left_child() || !right_child()) {
    return;
  }

  /**
   * Collect the output column names of the children on the fly, because the children might change.
   */
  const auto &left_names = left_child()->output_column_names();
  const auto &right_names = right_child()->output_column_names();

  _output_column_names.clear();
  _output_column_names.reserve(left_names.size() + right_names.size());

  _output_column_names.insert(_output_column_names.end(), left_names.begin(), left_names.end());
  _output_column_names.insert(_output_column_names.end(), right_names.begin(), right_names.end());

  /**
   * Collect the output ColumnIDs of the children on the fly, because the children might change.
   */
  const auto num_left_columns = left_child()->num_output_columns();
  const auto num_right_columns = right_child()->num_output_columns();

  _output_column_id_to_input_column_id.clear();
  _output_column_id_to_input_column_id.reserve(num_left_columns + num_right_columns);

  for (ColumnID column_id{0}; column_id < num_left_columns; ++column_id) {
    _output_column_id_to_input_column_id.emplace_back(column_id);
  }

  for (ColumnID column_id{0}; column_id < num_right_columns; ++column_id) {
    _output_column_id_to_input_column_id.emplace_back(column_id);
  }
}

}  // namespace opossum
