#include "join_node.hpp"

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

namespace opossum {

JoinNode::JoinNode(const JoinMode join_mode) : AbstractLQPNode(LQPNodeType::Join), _join_mode(join_mode) {
  DebugAssert(join_mode == JoinMode::Cross || join_mode == JoinMode::Natural,
              "Specified JoinMode must also specify column ids and scan type.");
}

JoinNode::JoinNode(const JoinMode join_mode, const JoinColumnOrigins& join_column_origins, const ScanType scan_type)
    : AbstractLQPNode(LQPNodeType::Join),
      _join_mode(join_mode),
      _join_column_origins(join_column_origins),
      _scan_type(scan_type) {
  DebugAssert(join_mode != JoinMode::Cross && join_mode != JoinMode::Natural,
              "Specified JoinMode must specify neither column ids nor scan type.");
}

std::shared_ptr<AbstractLQPNode> JoinNode::_deep_copy_impl(const std::shared_ptr<AbstractLQPNode>& left_child,
                                                           const std::shared_ptr<AbstractLQPNode>& right_child) const {
  if (_join_mode == JoinMode::Cross || _join_mode == JoinMode::Natural) {
    return std::make_shared<JoinNode>(_join_mode);
  } else {
    Assert(this->left_child(), "Can't clone without child");

    const auto column_origins = JoinColumnOrigins{
        this->left_child()->deep_copy_column_origin(_join_column_origins->first, left_child),
        this->right_child()->deep_copy_column_origin(_join_column_origins->first, right_child),
    };
    return std::make_shared<JoinNode>(_join_mode, column_origins, *_scan_type);
  }
}

std::string JoinNode::description() const {
  Assert(left_child() && right_child(), "Can't generate description if children aren't set");

  std::ostringstream desc;

  desc << "[" << join_mode_to_string.at(_join_mode) << " Join]";

  if (_join_column_origins && _scan_type) {
    desc << " " << _join_column_origins->first.get_verbose_name();
    desc << " " << scan_type_to_string.left.at(*_scan_type);
    desc << " " << _join_column_origins->second.get_verbose_name();
  }

  return desc.str();
}

const std::vector<std::string>& JoinNode::output_column_names() const {
  if (!_output_column_names) {
    _update_output();
  }

  return *_output_column_names;
}

const std::vector<LQPColumnOrigin>& JoinNode::output_column_origins() const {
  if (!_output_column_origins) {
    _update_output();
  }

  return *_output_column_origins;
}

std::shared_ptr<TableStatistics> JoinNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const {
  if (_join_mode == JoinMode::Cross) {
    return left_child->get_statistics()->generate_cross_join_statistics(right_child->get_statistics());
  } else {
    Assert(_join_column_origins,
           "Only cross joins and joins with join column ids supported for generating join statistics");
    Assert(_scan_type, "Only cross joins and joins with scan type supported for generating join statistics");

    JoinColumnIDs join_colum_ids{left_child->get_output_column_id_by_column_origin(_join_column_origins->first),
                                 right_child->get_output_column_id_by_column_origin(_join_column_origins->second)};

    return left_child->get_statistics()->generate_predicated_join_statistics(right_child->get_statistics(), _join_mode,
                                                                             join_colum_ids, *_scan_type);
  }
}

const std::optional<JoinColumnOrigins>& JoinNode::join_column_origins() const { return _join_column_origins; }

const std::optional<ScanType>& JoinNode::scan_type() const { return _scan_type; }

JoinMode JoinNode::join_mode() const { return _join_mode; }

std::string JoinNode::get_verbose_column_name(ColumnID column_id) const {
  Assert(left_child() && right_child(), "Can't generate column names without children being set");

  if (column_id < left_child()->output_column_count()) {
    return left_child()->get_verbose_column_name(column_id);
  }
  return right_child()->get_verbose_column_name(static_cast<ColumnID>(column_id - left_child()->output_column_count()));
}

void JoinNode::_on_child_changed() { _output_column_names.reset(); }

void JoinNode::_update_output() const {
  /**
   * The output (column names and output-to-input mapping) of this node gets cleared whenever a child changed and is
   * re-computed on request. This allows LQPs to be in temporary invalid states (e.g. no left child in Join) and thus
   * allows easier manipulation in the optimizer.
   */

  DebugAssert(left_child() && right_child(), "Need both inputs to compute output");

  /**
   * Collect the output column names of the children on the fly, because the children might change.
   */
  const auto& left_names = left_child()->output_column_names();
  const auto& right_names = right_child()->output_column_names();

  _output_column_names.emplace();
  _output_column_names->reserve(left_names.size() + right_names.size());

  _output_column_names->insert(_output_column_names->end(), left_names.begin(), left_names.end());
  _output_column_names->insert(_output_column_names->end(), right_names.begin(), right_names.end());

  /**
   * Collect the output ColumnIDs of the children on the fly, because the children might change.
   */
  _output_column_origins.emplace();

  _output_column_origins->insert(_output_column_origins->end(), left_child()->output_column_origins().begin(),
                                 left_child()->output_column_origins().end());
  _output_column_origins->insert(_output_column_origins->end(), right_child()->output_column_origins().begin(),
                                 right_child()->output_column_origins().end());
}

}  // namespace opossum
