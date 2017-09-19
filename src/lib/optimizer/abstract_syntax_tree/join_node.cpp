#include "join_node.hpp"

#include <limits>
#include <numeric>
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

optional<ColumnID> JoinNode::find_column_id_by_named_column_reference(
    const NamedColumnReference &named_column_reference) const {
  DebugAssert(left_child() && right_child(), "JoinNode must have two children.");

  optional<ColumnID> left_column_id;
  optional<ColumnID> right_column_id;

  // If there is no qualifying table name, search both children.
  if (!named_column_reference.table_name) {
    left_column_id = left_child()->find_column_id_by_named_column_reference(named_column_reference);
    right_column_id = right_child()->find_column_id_by_named_column_reference(named_column_reference);
  } else {
    // Otherwise only search a child if it knows that qualifier.
    auto left_knows_table = left_child()->knows_table(*named_column_reference.table_name);
    auto right_knows_table = right_child()->knows_table(*named_column_reference.table_name);

    // If neither input table knows the table name, return.
    if (!left_knows_table && !right_knows_table) {
      return nullopt;
    }

    // There must not be two tables with the same qualifying name.
    Assert(left_knows_table ^ right_knows_table, "Table name " + *named_column_reference.table_name + " is ambiguous.");

    if (left_knows_table) {
      left_column_id = left_child()->find_column_id_by_named_column_reference(named_column_reference);
    } else {
      right_column_id = right_child()->find_column_id_by_named_column_reference(named_column_reference);
    }
  }

  // If neither input table has that column, return.
  if (!left_column_id && !right_column_id) {
    return nullopt;
  }

  Assert(static_cast<bool>(left_column_id) ^ static_cast<bool>(right_column_id),
         "Column name " + named_column_reference.column_name + " is ambiguous.");

  ColumnID input_column_id;
  ColumnID output_column_id;

  if (left_column_id) {
    input_column_id = *left_column_id;
    output_column_id = *left_column_id;
  } else {
    input_column_id = *right_column_id;
    output_column_id = left_child()->output_col_count() + *right_column_id;
  }

  DebugAssert(_output_column_id_to_input_column_id[output_column_id] == input_column_id,
              "ColumnID should be in output.");

  return output_column_id;
}

bool JoinNode::knows_table(const std::string &table_name) const {
  DebugAssert(left_child() && right_child(), "JoinNode must have two children.");
  return left_child()->knows_table(table_name) || right_child()->knows_table(table_name);
}

std::vector<ColumnID> JoinNode::get_output_column_ids_for_table(const std::string &table_name) const {
  DebugAssert(left_child() && right_child(), "JoinNode must have two children.");

  auto left_knows_table = left_child()->knows_table(table_name);
  auto right_knows_table = right_child()->knows_table(table_name);

  // If neither input table knows the table name, return.
  if (!left_knows_table && !right_knows_table) {
    return {};
  }

  // There must not be two tables with the same qualifying name.
  Assert(left_knows_table ^ right_knows_table, "Table name " + table_name + " is ambiguous.");

  if (left_knows_table) {
    // The ColumnIDs of the left table appear first in `_output_column_id_to_input_column_id`.
    // That means they are at the same position in our output, so we can return them directly.
    return left_child()->get_output_column_ids_for_table(table_name);
  }

  // The ColumnIDs of the right table appear after the ColumnIDs of the left table.
  // Add that offset to each of them and return that list.
  const auto input_column_ids_for_table = right_child()->get_output_column_ids_for_table(table_name);
  std::vector<ColumnID> output_column_ids_for_table;
  for (const auto input_column_id : input_column_ids_for_table) {
    const auto idx = left_child()->output_col_count() + input_column_id;
    output_column_ids_for_table.emplace_back(static_cast<ColumnID::base_type>(idx));
  }

  return output_column_ids_for_table;
}

const optional<std::pair<ColumnID, ColumnID>> &JoinNode::join_column_ids() const { return _join_column_ids; }

const optional<ScanType> &JoinNode::scan_type() const { return _scan_type; }

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
  const auto num_left_columns = left_child()->output_col_count();
  const auto num_right_columns = right_child()->output_col_count();

  _output_column_id_to_input_column_id.clear();
  _output_column_id_to_input_column_id.resize(num_left_columns + num_right_columns);

  auto begin = _output_column_id_to_input_column_id.begin();
  std::iota(begin, begin + num_left_columns, 0);
  std::iota(begin + num_left_columns, _output_column_id_to_input_column_id.end(), 0);
}

}  // namespace opossum
