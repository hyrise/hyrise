#include "join_to_predicate_rewrite_rule.hpp"

#include <magic_enum.hpp>

#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

std::string JoinToPredicateRewriteRule::name() const {
  static const auto name = std::string{"JoinToPredicateRewriteRule"};
  return name;
}

void JoinToPredicateRewriteRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // rewritables finally contains all rewritable join nodes, there unused input side and the predicate to be used for
  // the rewrite.
  auto rewritables = std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<PredicateNode>>>{};

  visit_lqp(lqp_root, [&](const auto& node) {
    if (node->type == LQPNodeType::Join) {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      _gather_rewrite_info(join_node, rewritables);
    }
    return LQPVisitation::VisitInputs;
  });

  for (const auto& rewrite_candidate : rewritables) {
    _perform_rewrite(std::get<0>(rewrite_candidate), std::get<1>(rewrite_candidate), std::get<2>(rewrite_candidate));
  }
}

void JoinToPredicateRewriteRule::_gather_rewrite_info(
    const std::shared_ptr<JoinNode>& join_node,
    std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<PredicateNode>>>& rewritables) {
  const auto removable_side = join_node->get_unused_input();
  if (!removable_side) {
    return;
  }

  auto removable_subtree = std::shared_ptr<AbstractLQPNode>{};
  auto valid_predicate = std::shared_ptr<PredicateNode>{};

  removable_subtree = join_node->input(*removable_side);

  // We don't handle the more complicated case of multiple join predicates. Rewriting in these cases would require
  // finding and combining multiple suitable predicates. We also don't rewrite anti-joins.
  if (join_node->join_predicates().size() != 1 || join_node->join_mode == JoinMode::AntiNullAsTrue ||
      join_node->join_mode == JoinMode::AntiNullAsFalse) {
    return;
  }

  const auto& join_predicates = join_node->join_predicates();
  const auto& join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());

  Assert(join_predicate, "A join must have at least one BinaryPredicateExpression.");

  std::shared_ptr<AbstractExpression> exchangable_column_expr = nullptr;

  if (expression_evaluable_on_lqp(join_predicate->left_operand(), *removable_subtree)) {
    exchangable_column_expr = join_predicate->left_operand();
  } else if (expression_evaluable_on_lqp(join_predicate->right_operand(), *removable_subtree)) {
    exchangable_column_expr = join_predicate->right_operand();
  }

  Assert(exchangable_column_expr, "Neither column of the join predicate could be evaluated on the removable input.");

  // Check for uniqueness
  const auto testable_expressions = ExpressionUnorderedSet{exchangable_column_expr};

  if (!removable_subtree->has_matching_unique_constraint(testable_expressions)) {
    return;
  }

  // Now, we look for a predicate that can be used inside the substituting table scan node.
  // If we find an equals-predicate that filters on a UCC, a maximum of one line is singled out in the result relation.
  // Since at this point, we already know the join in question is basically a semi-join, we can further transform the
  // join to a single predicate node filtering out this one line testing against values in the join column.
  visit_lqp(removable_subtree, [&removable_subtree, &valid_predicate](auto& current_node) {
    if (current_node->type == LQPNodeType::Union) {
      return LQPVisitation::DoNotVisitInputs;
    }
    if (current_node->type != LQPNodeType::Predicate) {
      return LQPVisitation::VisitInputs;
    }

    const auto candidate = std::static_pointer_cast<PredicateNode>(current_node);
    const auto candidate_exp = std::dynamic_pointer_cast<BinaryPredicateExpression>(candidate->predicate());

    if (!candidate_exp) {
      return LQPVisitation::VisitInputs;
    }

    Assert(candidate_exp, "Need to get a predicate with a BinaryPredicateExpression.");

    // Only predicates that filter using equals cond. on a column with a constant value are of use to our optimization.
    // These conditions have the potential (given filtered column is a UCC) to single out a maximum of one result tuple.
    if (candidate_exp->predicate_condition != PredicateCondition::Equals)
      return LQPVisitation::VisitInputs;

    std::shared_ptr<AbstractExpression> candidate_column_expr = nullptr;
    std::shared_ptr<AbstractExpression> candidate_value_expr = nullptr;

    for (const auto& predicate_operand : {candidate_exp->left_operand(), candidate_exp->right_operand()}) {
      if (predicate_operand->type == ExpressionType::LQPColumn) {
        candidate_column_expr = predicate_operand;
      } else if (predicate_operand->type == ExpressionType::Value) {
        candidate_value_expr = predicate_operand;
      }
    }

    // There should not be a case where we run into no column, but there may be a case where we have no value but
    // compare columns.
    if (!candidate_column_expr || !candidate_value_expr) {
      return LQPVisitation::VisitInputs;
    }

    // If the Predicate column expr != join column expression, check whether the column referenced is still available
    // in the join.
    if (!expression_evaluable_on_lqp(candidate_column_expr, *removable_subtree)) {
      return LQPVisitation::VisitInputs;
    }

    // Check for uniqueness.
    const auto testable_expressions = ExpressionUnorderedSet{candidate_column_expr};

    if (!removable_subtree->has_matching_unique_constraint(testable_expressions)) {
      return LQPVisitation::VisitInputs;
    }

    valid_predicate = candidate;
    return LQPVisitation::DoNotVisitInputs;
  });

  if (valid_predicate) {
    rewritables.emplace_back(join_node, removable_side.value(), valid_predicate);
  }
}

void JoinToPredicateRewriteRule::_perform_rewrite(const std::shared_ptr<JoinNode>& join_node,
                                                  const LQPInputSide& removable_side,
                                                  const std::shared_ptr<PredicateNode>& valid_predicate) {
  if (removable_side == LQPInputSide::Left) {
    join_node->set_left_input(join_node->right_input());
  }
  join_node->set_right_input(nullptr);

  // Get the join predicate, as we need to extract which column to filter on.
  // We ensured before that there is exactly one predicate for the current join.
  const auto& join_predicates = join_node->join_predicates();
  const auto& join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());

  Assert(join_predicate, "A Join must have at least one BinaryPredicateExpression.");

  std::shared_ptr<AbstractExpression> used_join_column = nullptr;
  std::shared_ptr<AbstractExpression> projection_column = nullptr;

  if (expression_evaluable_on_lqp(join_predicate->left_operand(), *join_node->left_input())) {
    used_join_column = join_predicate->left_operand();
    projection_column = join_predicate->right_operand();
  } else if (expression_evaluable_on_lqp(join_predicate->right_operand(), *join_node->left_input())) {
    used_join_column = join_predicate->right_operand();
    projection_column = join_predicate->left_operand();
  }

  const auto projections = expression_vector(projection_column);
  auto projection_node = ProjectionNode::make(projections, valid_predicate);
  auto replacement_predicate_node = PredicateNode::make(equals_(used_join_column, lqp_subquery_(projection_node)));

  lqp_replace_node(join_node, replacement_predicate_node);
}

}  // namespace hyrise
