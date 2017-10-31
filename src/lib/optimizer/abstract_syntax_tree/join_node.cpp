#include "join_node.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "optimizer/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/type_utils.hpp"

namespace opossum {

JoinNode::JoinNode(const JoinMode join_mode) : AbstractASTNode(ASTNodeType::Join), _join_mode(join_mode) {
  DebugAssert(join_mode == JoinMode::Cross || join_mode == JoinMode::Natural,
              "Specified JoinMode must also specify column ids and scan type.");
}

JoinNode::JoinNode(const JoinMode join_mode, const std::pair<ColumnID, ColumnID>& join_column_ids,
                   const ScanType scan_type)
    : AbstractASTNode(ASTNodeType::Join),
      _join_mode(join_mode),
      _join_column_ids(join_column_ids),
      _scan_type(scan_type) {
  DebugAssert(join_mode != JoinMode::Cross && join_mode != JoinMode::Natural,
              "Specified JoinMode must specify neither column ids nor scan type.");
}

std::string JoinNode::description() const {
  Assert(left_child() && right_child(), "Can't generate description if children aren't set");

  std::ostringstream desc;

  desc << "[" << join_mode_to_string.at(_join_mode) << " Join]";

  if (_join_column_ids && _scan_type) {
    desc << " " << get_verbose_column_name(_join_column_ids->first);
    desc << " " << scan_type_to_string.left.at(*_scan_type);
    desc << " " << get_verbose_column_name(ColumnID{
                       static_cast<ColumnID::base_type>(left_child()->output_col_count() + _join_column_ids->second)});
  }

  return desc.str();
}

const std::vector<ColumnID>& JoinNode::output_column_id_to_input_column_id() const {
  return _output_column_id_to_input_column_id;
}

const std::vector<std::string>& JoinNode::output_column_names() const { return _output_column_names; }

std::optional<ColumnID> JoinNode::find_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  DebugAssert(left_child() && right_child(), "JoinNode must have two children.");

  auto named_column_reference_without_local_alias = _resolve_local_alias(named_column_reference);
  if (!named_column_reference_without_local_alias) {
    return {};
  }

  std::optional<ColumnID> left_column_id;
  std::optional<ColumnID> right_column_id;

  // If there is no qualifying table name or this table's alias is used, search both children.
  if (!named_column_reference_without_local_alias->table_name ||
      (_table_alias && *_table_alias == named_column_reference_without_local_alias->table_name)) {
    left_column_id =
        left_child()->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);
    right_column_id =
        right_child()->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);
  } else {
    // Otherwise only search a child if it knows that qualifier.
    auto left_knows_table = left_child()->knows_table(*named_column_reference_without_local_alias->table_name);
    auto right_knows_table = right_child()->knows_table(*named_column_reference_without_local_alias->table_name);

    // If neither input table knows the table name, return.
    if (!left_knows_table && !right_knows_table) {
      return std::nullopt;
    }

    // There must not be two tables with the same qualifying name.
    Assert(left_knows_table ^ right_knows_table,
           "Table name " + *named_column_reference_without_local_alias->table_name + " is ambiguous.");

    if (left_knows_table) {
      left_column_id =
          left_child()->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);
    } else {
      right_column_id =
          right_child()->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);
    }
  }

  // If neither input table has that column, return.
  if (!left_column_id && !right_column_id) {
    return std::nullopt;
  }

  Assert(static_cast<bool>(left_column_id) ^ static_cast<bool>(right_column_id),
         "Column name " + named_column_reference_without_local_alias->column_name + " is ambiguous.");

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

std::shared_ptr<TableStatistics> JoinNode::derive_statistics_from(
    const std::shared_ptr<AbstractASTNode>& left_child, const std::shared_ptr<AbstractASTNode>& right_child) const {
  if (_join_mode == JoinMode::Cross) {
    return left_child->get_statistics()->generate_cross_join_statistics(right_child->get_statistics());
  } else {
    Assert(_join_column_ids,
           "Only cross joins and joins with join column ids supported for generating join statistics");
    Assert(_scan_type, "Only cross joins and joins with scan type supported for generating join statistics");
    return left_child->get_statistics()->generate_predicated_join_statistics(right_child->get_statistics(), _join_mode,
                                                                             *_join_column_ids, *_scan_type);
  }
}

bool JoinNode::knows_table(const std::string& table_name) const {
  DebugAssert(left_child() && right_child(), "JoinNode must have two children.");
  if (_table_alias) {
    return *_table_alias == table_name;
  } else {
    return left_child()->knows_table(table_name) || right_child()->knows_table(table_name);
  }
}

std::vector<ColumnID> JoinNode::get_output_column_ids_for_table(const std::string& table_name) const {
  DebugAssert(left_child() && right_child(), "JoinNode must have two children.");

  if (_table_alias) {
    if (*_table_alias == table_name) {
      return get_output_column_ids();
    } else {
      // if the join is an aliased subquery, we cannot access the individual tables anymore.
      return {};
    }
  }

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

const std::optional<std::pair<ColumnID, ColumnID>>& JoinNode::join_column_ids() const { return _join_column_ids; }

const std::optional<ScanType>& JoinNode::scan_type() const { return _scan_type; }

JoinMode JoinNode::join_mode() const { return _join_mode; }

std::string JoinNode::get_verbose_column_name(ColumnID column_id) const {
  Assert(left_child() && right_child(), "Can't generate column names without children being set");

  if (column_id < left_child()->output_col_count()) {
    return left_child()->get_verbose_column_name(column_id);
  }
  return right_child()->get_verbose_column_name(
      ColumnID{static_cast<ColumnID::base_type>(column_id - left_child()->output_col_count())});
}

void JoinNode::_on_child_changed() {
  // Only set output information if both children have already been set.
  if (!left_child() || !right_child()) {
    return;
  }

  /**
   * Collect the output column names of the children on the fly, because the children might change.
   */
  const auto& left_names = left_child()->output_column_names();
  const auto& right_names = right_child()->output_column_names();

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

ColumnOrigin JoinNode::get_column_origin(ColumnID column_id) const {
  DebugAssert(left_child() && right_child(), "Need both children to determine column origin");
  if (static_cast<size_t>(column_id) >= left_child()->output_col_count()) {
    const auto right_column_id = make_column_id(column_id - left_child()->output_col_count());
    DebugAssert(static_cast<size_t>(right_column_id) < right_child()->output_col_count(), "ColumnID out of range");
    return right_child()->get_column_origin(right_column_id);
  }
  return left_child()->get_column_origin(column_id);
}

void JoinNode::map_column_ids(const ColumnIDMapping& column_id_mapping,
                              ASTChildSide caller_child_side) {
  DebugAssert(left_child() && right_child(), "Children need to be set for this operation");

  ColumnIDMapping join_column_id_mapping(output_col_count(), INVALID_COLUMN_ID);

  if (caller_child_side == ASTChildSide::Left) {
    DebugAssert(column_id_mapping.size() == left_child()->output_col_count(), "Invalid column_id_mapping");

    if (_join_column_ids) {
      _join_column_ids->first = column_id_mapping[_join_column_ids->first];
    }

    std::copy(column_id_mapping.begin(), column_id_mapping.end(), join_column_id_mapping.begin());
    std::iota(join_column_id_mapping.begin() + left_child()->output_col_count(), join_column_id_mapping.end(),
              make_column_id(left_child()->output_col_count()));
  } else {
    DebugAssert(column_id_mapping.size() == right_child()->output_col_count(), "Invalid column_id_mapping");

    if (_join_column_ids) {
      _join_column_ids->second = column_id_mapping[_join_column_ids->second];
    }
    std::iota(join_column_id_mapping.begin(), join_column_id_mapping.begin() + left_child()->output_col_count(),
              ColumnID{0});
    const auto left_column_count = left_child()->output_col_count();
    const auto join_column_count = output_col_count();

    for (size_t join_column_idx = left_column_count; join_column_idx < join_column_count; ++join_column_idx) {
      join_column_id_mapping[join_column_idx] =
          column_id_mapping[join_column_idx - left_column_count] + left_column_count;
    }
  }

  _propagate_column_id_mapping_to_parent(join_column_id_mapping);
}

}  // namespace opossum
