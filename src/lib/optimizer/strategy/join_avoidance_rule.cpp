#include "join_avoidance_rule.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"

namespace {

using namespace hyrise;                         // NOLINT(build/namespaces)
using namespace hyrise::expression_functional;  // NOLINT(build/namespaces)

void recursively_remove_joins(const std::shared_ptr<AbstractLQPNode>& node,
                               std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!node || visited_nodes.contains(node)) {
    return;
  }

  visited_nodes.emplace(node);

  if (node->type != LQPNodeType::Join) {
    recursively_remove_joins(node->left_input(), visited_nodes);
    recursively_remove_joins(node->right_input(), visited_nodes);
    return;
  }
  

  const auto& join_node = std::static_pointer_cast<JoinNode>(node);
  const auto prunable_side = join_node->prunable_input_side();
  if (!prunable_side) {
    recursively_remove_joins(node->left_input(), visited_nodes);
    recursively_remove_joins(node->right_input(), visited_nodes);
    return;
  }

  // We don't handle the more complicated case of multiple join predicates. Rewriting in these cases would require
  // finding and combining multiple suitable predicates. We also only rewrite inner and semi joins. However,
  // AntiNullAsFalse joins could be rewritten to a scan with PredicateCondition::NotEquals. We cannot rewrite outer or
  // cross joins, as the results of the original and the rewritten query plan would not be equal.
  const auto& join_predicates = join_node->join_predicates();
  if (join_predicates.size() != 1 ||
      (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Semi)) {
    recursively_remove_joins(node->left_input(), visited_nodes);
    recursively_remove_joins(node->right_input(), visited_nodes);
    return;
  }

  const auto& join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());
  Assert(join_predicate, "A join must have at least one BinaryPredicateExpression.");

  const auto& removable_subtree = join_node->input(*prunable_side);
  auto rewrite_predicate = std::shared_ptr<PredicateNode>{};
  auto exchangeable_column_expression = std::shared_ptr<AbstractExpression>{};
  auto used_join_column_expression = std::shared_ptr<AbstractExpression>{};

  if (expression_evaluable_on_lqp(join_predicate->left_operand(), *removable_subtree)) {
    exchangeable_column_expression = join_predicate->left_operand();
    used_join_column_expression = join_predicate->right_operand();
  } else if (expression_evaluable_on_lqp(join_predicate->right_operand(), *removable_subtree)) {
    exchangeable_column_expression = join_predicate->right_operand();
    used_join_column_expression = join_predicate->left_operand();
  }

  Assert(exchangeable_column_expression,
         "Neither column of the join predicate could be evaluated on the removable input.");


  // Test that exchangeable_column_expression is unique. Otherwise, there could be multiple matches and we must perform
  // the original join.
  if (join_node->join_mode == JoinMode::Inner &&
      !join_node->input(*prunable_side)->has_matching_ucc({exchangeable_column_expression})) {
    recursively_remove_joins(node->left_input(), visited_nodes);
    recursively_remove_joins(node->right_input(), visited_nodes);
    return;
  }

  // To ensure that all tuples match, there must be an IND.
  if (!join_node->input(*prunable_side)->has_matching_ind({used_join_column_expression}, {exchangeable_column_expression}, *join_node)) {
    recursively_remove_joins(node->left_input(), visited_nodes);
    recursively_remove_joins(node->right_input(), visited_nodes);
    return;
  }

  const auto used_subtree = join_node->input(*prunable_side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left);
  // std::cout << *join_node << std::endl << std::endl << *used_subtree << std::endl;
  join_node->set_right_input(nullptr);
  join_node->set_left_input(used_subtree);

  lqp_remove_node(join_node);
  if (used_join_column_expression->is_nullable_on_lqp(*used_subtree)) {
    /*auto value = AllTypeVariant();

    resolve_data_type(used_join_column_expression->data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        value = pmr_string{};
      } else {
        value = std::numeric_limits<ColumnDataType>::min();
      }
    });*/
    // const auto predicate_node = PredicateNode::make(greater_than_equals_(used_join_column_expression, value));
    const auto predicate_node = PredicateNode::make(is_not_null_(used_join_column_expression));
    lqp_insert_node_above(used_subtree, predicate_node);
    // std::cout << "O-4: rewrite " << join_node->description() << " to " << predicate_node->description() << std::endl;
  } /*else {
    std::cout << "O-4: remove " << join_node->description() << std::endl;
  }*/
  recursively_remove_joins(used_subtree, visited_nodes);
}

}  // namespace

namespace hyrise {

std::string JoinAvoidanceRule::name() const {
  static const auto name = std::string{"JoinAvoidanceRule"};
  return name;
}

void JoinAvoidanceRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // We cannot use visit_lqp(...) since we replace nodes in the plan. The visited nodes may be removed from the plan,
  // will not have inputs anymore, and the LQP traversal would stop after one successful rewrite.
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  recursively_remove_joins(lqp_root, visited_nodes);
}

}  // namespace hyrise
