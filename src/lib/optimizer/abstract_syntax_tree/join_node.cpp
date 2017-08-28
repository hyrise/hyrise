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

optional<ColumnID> JoinNode::find_column_id_for_column_identifier(const ColumnIdentifier &column_identifier) const {
  DebugAssert(!!left_child() && !!right_child(), "JoinNode must have two children.");

  // If there is no qualifying table name, search both children.
  if (!column_identifier.table_name) {
    const auto left_column_id = left_child()->find_column_id_for_column_identifier(column_identifier);
    const auto right_column_id = right_child()->find_column_id_for_column_identifier(column_identifier);

    Assert(static_cast<bool>(left_column_id) ^ static_cast<bool>(right_column_id),
           "Column name " + column_identifier.column_name + " is ambiguous.");

    if (left_column_id) {
      return left_column_id;
    }

    auto column_idx = left_child()->output_column_ids().size() + (*right_column_id);
    DebugAssert(column_idx < std::numeric_limits<uint16_t>::max(), "Too many columns for table.");

    return ColumnID{static_cast<ColumnID::base_type>(column_idx)};
  }

  // There must not be two tables with the same qualifying name.
  Assert(left_child()->manages_table(*column_identifier.table_name) ^
             right_child()->manages_table(*column_identifier.table_name),
         "Table name " + *column_identifier.table_name + " is ambiguous.");

  // Otherwise only search a children if it manages that qualifier.
  if (left_child()->manages_table(*column_identifier.table_name)) {
    return left_child()->find_column_id_for_column_identifier(column_identifier);
  }

  if (right_child()->manages_table(*column_identifier.table_name)) {
    auto column_id = right_child()->find_column_id_for_column_identifier(column_identifier);
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
