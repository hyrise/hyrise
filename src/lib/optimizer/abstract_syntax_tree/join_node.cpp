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

const std::vector<ColumnID> &JoinNode::output_column_ids() const { return _output_column_ids; }

const std::vector<std::string> &JoinNode::output_column_names() const { return _output_column_names; }

optional<ColumnID> JoinNode::find_column_id_for_column_identifier_name(
    const ColumnIdentifierName &column_identifier_name) const {
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");

  // If there is no qualifying table name, search both children.
  if (!column_identifier_name.table_name) {
    const auto left_column_id = left_child()->find_column_id_for_column_identifier_name(column_identifier_name);
    const auto right_column_id = right_child()->find_column_id_for_column_identifier_name(column_identifier_name);

    // If neither input table has that column, return.
    if (!left_column_id && !left_column_id) {
      return nullopt;
    }

    Assert(static_cast<bool>(left_column_id) ^ static_cast<bool>(right_column_id),
           "Column name " + column_identifier_name.column_name + " is ambiguous.");

    if (left_column_id) {
      return left_column_id;
    }

    auto column_idx = left_child()->output_column_ids().size() + (*right_column_id);
    DebugAssert(column_idx < std::numeric_limits<uint16_t>::max(), "Too many columns for table.");

    return ColumnID{static_cast<ColumnID::base_type>(column_idx)};
  }

  auto left_manages_table = left_child()->manages_table(*column_identifier_name.table_name);
  auto right_manages_table = right_child()->manages_table(*column_identifier_name.table_name);

  // If neither input table manages the table name, return.
  if (!left_manages_table && !right_manages_table) {
    return nullopt;
  }

  // There must not be two tables with the same qualifying name.
  Assert(left_manages_table ^ right_manages_table,
         "Table name " + *column_identifier_name.table_name + " is ambiguous.");

  // Otherwise only search a children if it manages that qualifier.
  if (left_manages_table) {
    return left_child()->find_column_id_for_column_identifier_name(column_identifier_name);
  }

  if (right_manages_table) {
    auto column_id = right_child()->find_column_id_for_column_identifier_name(column_identifier_name);
    if (column_id) {
      auto column_idx = left_child()->output_column_ids().size() + (*column_id);
      DebugAssert(column_idx < std::numeric_limits<uint16_t>::max(), "Too many columns for table.");
      return ColumnID{static_cast<ColumnID::base_type>(column_idx)};
    }
  }

  return nullopt;
}

bool JoinNode::manages_table(const std::string &table_name) const {
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");
  return left_child()->manages_table(table_name) || right_child()->manages_table(table_name);
}

std::vector<ColumnID> JoinNode::get_column_ids_for_table(const std::string &table_name) const {
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");

  auto left_manages_table = left_child()->manages_table(table_name);
  auto right_manages_table = right_child()->manages_table(table_name);

  // If neither input table manages the table name, return.
  if (!left_manages_table && !right_manages_table) {
    return {};
  }

  // There must not be two tables with the same qualifying name.
  Assert(left_manages_table ^ right_manages_table, "Table name " + table_name + " is ambiguous.");

  std::vector<ColumnID> input_column_ids_for_table;
  if (left_manages_table) {
    input_column_ids_for_table = left_child()->get_column_ids_for_table(table_name);
  } else {
    // Add offset of columns from left child to ColumnIDs.
    auto original_right_input_column_ids_for_table = right_child()->get_column_ids_for_table(table_name);
    input_column_ids_for_table.reserve(original_right_input_column_ids_for_table.size());

    for (const auto column_id : original_right_input_column_ids_for_table) {
      auto column_idx = left_child()->output_column_ids().size() + column_id;
      DebugAssert(column_idx < std::numeric_limits<uint16_t>::max(), "Too many columns for table.");
      input_column_ids_for_table.emplace_back(ColumnID{static_cast<ColumnID::base_type>(column_idx)});
    }
  }

  // Return only the offsets of those ColumnIDs that appear in the output.
  const auto &all_output_column_ids = output_column_ids();
  std::vector<ColumnID> output_column_ids_for_table;

  for (const auto input_column_id : input_column_ids_for_table) {
    const auto iter = std::find(all_output_column_ids.begin(), all_output_column_ids.end(), input_column_id);

    if (iter != all_output_column_ids.end()) {
      const auto idx = std::distance(all_output_column_ids.begin(), iter);
      output_column_ids_for_table.emplace_back(static_cast<ColumnID::base_type>(idx));
    }
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
  _output_column_ids.clear();
  _output_column_ids.reserve(left_child()->output_column_ids().size() + right_child()->output_column_ids().size());

  for (ColumnID column_id{0}; column_id < _output_column_ids.capacity(); column_id++) {
    _output_column_ids.emplace_back(column_id);
  }
}

}  // namespace opossum
