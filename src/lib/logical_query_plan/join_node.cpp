#include "join_node.hpp"

#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression_utils.hpp"
#include "constant_mappings.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinNode::JoinNode(const JoinMode join_mode):
AbstractLQPNode(LQPNodeType::Join), join_mode(join_mode) {
  Assert(join_mode == JoinMode::Cross, "Only Cross Joins can be constructed without predicate");
}

JoinNode::JoinNode(const JoinMode join_mode, const std::shared_ptr<AbstractExpression>& join_predicate):
AbstractLQPNode(LQPNodeType::Join), join_mode(join_mode), join_predicate(join_predicate) {
  Assert(join_mode != JoinMode::Cross , "Cross Joins take no predicate");
}

std::string JoinNode::description() const {
  std::stringstream stream;
  stream << "[Join] Mode: " << join_mode_to_string.at(join_mode);

  if (join_predicate) stream << " " << join_predicate->as_column_name();

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::output_column_expressions() const {
  Assert(left_input() && right_input(), "Both inputs need to be set to determine a JoiNode's output expressions");

  /**
   * Update the JoinNode's output expressions every time they are requested. An overhead, but keeps the LQP code simple.
   * Previously we propagated _input_changed() calls through the LQP every time a node changed and that required a lot
   * of feeble code.
   */

  const auto& left_expressions = left_input()->output_column_expressions();
  const auto& right_expressions = right_input()->output_column_expressions();

  _output_column_expressions.resize(left_expressions.size() + right_expressions.size());

  auto right_begin = std::copy(left_expressions.begin(), left_expressions.end(), _output_column_expressions.begin());
  std::copy(right_expressions.begin(), right_expressions.end(), right_begin);

  return _output_column_expressions;
}

std::shared_ptr<AbstractLQPNode> JoinNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  if (join_predicate) {
    return JoinNode::make(join_mode, expression_copy_and_adapt_to_different_lqp(*join_predicate, node_mapping));
  } else {
    return JoinNode::make(join_mode);
  }
}

bool JoinNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  const auto& join_node = static_cast<const JoinNode&>(rhs);

  if ((join_predicate == nullptr) != (join_node.join_predicate == nullptr)) return false;
  if (join_mode != join_node.join_mode) return false;
  if (!join_predicate && !join_node.join_predicate) return true;

  return expression_equal_to_expression_in_different_lqp(*join_predicate, *join_node.join_predicate, node_mapping);
}

}  // namespace opossum
