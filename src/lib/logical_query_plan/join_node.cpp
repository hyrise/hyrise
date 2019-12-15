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
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "operators/operator_join_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinNode::JoinNode(const JoinMode join_mode) : AbstractLQPNode(LQPNodeType::Join), join_mode(join_mode) {
  Assert(join_mode == JoinMode::Cross, "Only Cross Joins can be constructed without predicate");
}

JoinNode::JoinNode(const JoinMode join_mode, const std::shared_ptr<AbstractExpression>& join_predicate)
    : JoinNode(join_mode, std::vector<std::shared_ptr<AbstractExpression>>{join_predicate}) {}

JoinNode::JoinNode(const JoinMode join_mode, const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates)
    : AbstractLQPNode(LQPNodeType::Join, join_predicates), join_mode(join_mode) {
  Assert(join_mode != JoinMode::Cross, "Cross Joins take no predicate");
  Assert(!join_predicates.empty(), "Non-Cross Joins require predicates");
}

std::string JoinNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);

  std::stringstream stream;
  stream << "[Join] Mode: " << join_mode;

  for (const auto& predicate : join_predicates()) {
    stream << " [" << predicate->description(expression_mode) << "]";
  }

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::column_expressions() const {
  Assert(left_input() && right_input(), "Both inputs need to be set to determine a JoinNode's output expressions");

  /**
   * Update the JoinNode's output expressions every time they are requested. An overhead, but keeps the LQP code simple.
   * Previously we propagated _input_changed() calls through the LQP every time a node changed and that required a lot
   * of feeble code.
   */

  const auto& left_expressions = left_input()->column_expressions();
  const auto& right_expressions = right_input()->column_expressions();

  const auto output_both_inputs =
      join_mode != JoinMode::Semi && join_mode != JoinMode::AntiNullAsTrue && join_mode != JoinMode::AntiNullAsFalse;

  _column_expressions.resize(left_expressions.size() + (output_both_inputs ? right_expressions.size() : 0));

  auto right_begin = std::copy(left_expressions.begin(), left_expressions.end(), _column_expressions.begin());

  if (output_both_inputs) std::copy(right_expressions.begin(), right_expressions.end(), right_begin);

  return _column_expressions;
}

bool JoinNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  const auto left_input_column_count = left_input()->column_expressions().size();
  const auto column_is_from_left_input = column_id < left_input_column_count;

  if (join_mode == JoinMode::Left && !column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::Right && column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::FullOuter) {
    return true;
  }

  if (column_is_from_left_input) {
    return left_input()->is_column_nullable(column_id);
  } else {
    ColumnID right_column_id =
        static_cast<ColumnID>(column_id - static_cast<ColumnID::base_type>(left_input_column_count));
    return right_input()->is_column_nullable(right_column_id);
  }
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::join_predicates() const { return node_expressions; }

size_t JoinNode::_on_shallow_hash() const { return boost::hash_value(join_mode); }

std::shared_ptr<AbstractLQPNode> JoinNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  if (!join_predicates().empty()) {
    return JoinNode::make(join_mode, expressions_copy_and_adapt_to_different_lqp(join_predicates(), node_mapping));
  } else {
    return JoinNode::make(join_mode);
  }
}

bool JoinNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& join_node = static_cast<const JoinNode&>(rhs);
  if (join_mode != join_node.join_mode) return false;
  return expressions_equal_to_expressions_in_different_lqp(join_predicates(), join_node.join_predicates(),
                                                           node_mapping);
}

}  // namespace opossum
