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
              "Specified JoinMode must also specify column ids and predicate condition.");
}

JoinNode::JoinNode(const JoinMode join_mode, const LQPColumnReferencePair& join_column_references,
                   const PredicateCondition predicate_condition)
    : AbstractLQPNode(LQPNodeType::Join),
      _join_mode(join_mode),
      _join_column_references(join_column_references),
      _predicate_condition(predicate_condition) {
  DebugAssert(join_mode != JoinMode::Cross && join_mode != JoinMode::Natural,
              "Specified JoinMode must specify neither column ids nor predicate condition.");
}

std::shared_ptr<AbstractLQPNode> JoinNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  if (_join_mode == JoinMode::Cross || _join_mode == JoinMode::Natural) {
    return JoinNode::make(_join_mode);
  } else {
    Assert(left_input(), "Can't clone without input");

    const auto join_column_references = LQPColumnReferencePair{
        adapt_column_reference_to_different_lqp(_join_column_references->first, left_input(), copied_left_input),
        adapt_column_reference_to_different_lqp(_join_column_references->first, right_input(), copied_right_input),
    };
    return JoinNode::make(_join_mode, join_column_references, *_predicate_condition);
  }
}

std::string JoinNode::description() const {
  Assert(left_input() && right_input(), "Can't generate description if inputren aren't set");

  std::ostringstream desc;

  desc << "[" << join_mode_to_string.at(_join_mode) << " Join]";

  if (_join_column_references && _predicate_condition) {
    desc << " " << _join_column_references->first.description();
    desc << " " << predicate_condition_to_string.left.at(*_predicate_condition);
    desc << " " << _join_column_references->second.description();
  }

  return desc.str();
}

const std::vector<std::string>& JoinNode::output_column_names() const {
  if (!_output_column_names) {
    _update_output();
  }

  return *_output_column_names;
}

const std::vector<LQPColumnReference>& JoinNode::output_column_references() const {
  if (!_output_column_references) {
    _update_output();
  }

  return *_output_column_references;
}

std::shared_ptr<TableStatistics> JoinNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  if (_join_mode == JoinMode::Cross) {
    return left_input->get_statistics()->generate_cross_join_statistics(right_input->get_statistics());
  } else {
    Assert(_join_column_references,
           "Only cross joins and joins with join column ids supported for generating join statistics");
    Assert(_predicate_condition,
           "Only cross joins and joins with predicate condition supported for generating join statistics");

    ColumnIDPair join_colum_ids{left_input->get_output_column_id(_join_column_references->first),
                                right_input->get_output_column_id(_join_column_references->second)};

    return left_input->get_statistics()->generate_predicated_join_statistics(right_input->get_statistics(), _join_mode,
                                                                             join_colum_ids, *_predicate_condition);
  }
}

const std::optional<LQPColumnReferencePair>& JoinNode::join_column_references() const {
  return _join_column_references;
}

const std::optional<PredicateCondition>& JoinNode::predicate_condition() const { return _predicate_condition; }

JoinMode JoinNode::join_mode() const { return _join_mode; }

std::string JoinNode::get_verbose_column_name(ColumnID column_id) const {
  Assert(left_input() && right_input(), "Can't generate column names without inputren being set");

  if (column_id < left_input()->output_column_count()) {
    return left_input()->get_verbose_column_name(column_id);
  }
  return right_input()->get_verbose_column_name(static_cast<ColumnID>(column_id - left_input()->output_column_count()));
}

bool JoinNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& join_node = static_cast<const JoinNode&>(rhs);

  if (_join_mode != join_node._join_mode || _predicate_condition != join_node._predicate_condition) return false;
  if (_join_column_references.has_value() != join_node._join_column_references.has_value()) return false;

  if (!_join_column_references.has_value()) return true;

  return _equals(*this, _join_column_references->first, join_node, join_node._join_column_references->first) &&
         _equals(*this, _join_column_references->second, join_node, join_node._join_column_references->second);
}

void JoinNode::_on_input_changed() { _output_column_names.reset(); }

void JoinNode::_update_output() const {
  /**
   * The output (column names and output-to-input mapping) of this node gets cleared whenever a input changed and is
   * re-computed on request. This allows LQPs to be in temporary invalid states (e.g. no left input in Join) and thus
   * allows easier manipulation in the optimizer.
   */

  DebugAssert(left_input() && right_input(), "Need both inputs to compute output");

  /**
   * Collect the output column names of the inputren on the fly, because the inputren might change.
   */
  const auto& left_names = left_input()->output_column_names();
  const auto& right_names = right_input()->output_column_names();

  _output_column_names.emplace();
  const auto only_output_left_columns = _join_mode == JoinMode::Semi || _join_mode == JoinMode::Anti;

  const auto output_column_count =
      only_output_left_columns ? left_names.size() : left_names.size() + right_names.size();
  _output_column_names->reserve(output_column_count);

  _output_column_names->insert(_output_column_names->end(), left_names.begin(), left_names.end());

  /**
   * Collect the output ColumnIDs of the inputren on the fly, because the inputren might change.
   */
  _output_column_references.emplace();

  _output_column_references->insert(_output_column_references->end(), left_input()->output_column_references().begin(),
                                    left_input()->output_column_references().end());

  if (!only_output_left_columns) {
    _output_column_names->insert(_output_column_names->end(), right_names.begin(), right_names.end());
    _output_column_references->insert(_output_column_references->end(),
                                      right_input()->output_column_references().begin(),
                                      right_input()->output_column_references().end());
  }
}

}  // namespace opossum
