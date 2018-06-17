#include "join_node.hpp"

#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "expression/binary_predicate_expression.hpp"
#include "expression/lqp_column_expression.hpp"
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

std::shared_ptr<TableStatistics> JoinNode::derive_statistics_from(
const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(left_input && right_input, "JoinNode need left_input and no right_input");

  const auto cross_join_statistics = std::make_shared<TableStatistics>(left_input->get_statistics()->estimate_cross_join(*right_input->get_statistics()));

  if (join_mode == JoinMode::Cross) {
    return cross_join_statistics;

  } else {
    // If the JoinPredicate is a not simple `<column_a> <predicate_condition> <column_b>` predicate, then we have to
    // fall back to a selectivity of 1 (== CrossJoin) atm, because computing statistics for complex join predicates is
    // not implemented
    if (join_predicate->type != ExpressionType::Predicate) return cross_join_statistics;

    const auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);
    if (!binary_predicate) return cross_join_statistics;

    const auto left_column = std::dynamic_pointer_cast<LQPColumnExpression>(binary_predicate->left_operand());
    const auto right_column = std::dynamic_pointer_cast<LQPColumnExpression>(binary_predicate->right_operand());
    if (!left_column || !right_column) return cross_join_statistics;

    const auto predicate_condition = binary_predicate->predicate_condition;

    ColumnIDPair join_colum_ids{left_input->get_column_id(*left_column),
                                right_input->get_column_id(*right_column)};


    return std::make_shared<TableStatistics>(left_input->get_statistics()->estimate_predicated_join(
    *right_input->get_statistics(), join_mode, join_colum_ids, predicate_condition));
  }
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
