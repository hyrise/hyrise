#include "join_to_predicate_rewrite_rule.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace {

using namespace hyrise;                         // NOLINT(build/namespaces)
using namespace hyrise::expression_functional;  // NOLINT(build/namespaces)

void gather_rewrite_info(
    const std::shared_ptr<JoinNode>& join_node,
    std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<PredicateNode>>>& rewritables,
    bool& non_permanent_ucc_was_used) {
  const auto prunable_side = join_node->prunable_input_side();
  if (!prunable_side) {
    return;
  }

  // We don't handle the more complicated case of multiple join predicates. Rewriting in these cases would require
  // finding and combining multiple suitable predicates. We also only rewrite inner and semi joins. However,
  // AntiNullAsFalse joins could be rewritten to a scan with PredicateCondition::NotEquals. We cannot rewrite outer or
  // cross joins, as the results of the original and the rewritten query plan would not be equal.
  if (join_node->join_predicates().size() != 1 ||
      (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Semi)) {
    return;
  }

  const auto& join_predicates = join_node->join_predicates();
  const auto& join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());
  Assert(join_predicate, "A join must have at least one BinaryPredicateExpression.");

  auto removable_subtree = join_node->input(*prunable_side);
  auto rewrite_predicate = std::shared_ptr<PredicateNode>{};
  auto exchangeable_column_expression = std::shared_ptr<AbstractExpression>{};

  if (expression_evaluable_on_lqp(join_predicate->left_operand(), *removable_subtree)) {
    exchangeable_column_expression = join_predicate->left_operand();
  } else if (expression_evaluable_on_lqp(join_predicate->right_operand(), *removable_subtree)) {
    exchangeable_column_expression = join_predicate->right_operand();
  }

  Assert(exchangeable_column_expression,
         "Neither column of the join predicate could be evaluated on the removable input.");

  // Check for uniqueness.
  if (!removable_subtree->get_matching_ucc({exchangeable_column_expression}).has_value()) {
    return;
  }

  // Now, we look for a predicate that can potentially be used in a subquery to replace the join. If we find an equals
  // predicate that filters on a UCC, a maximum of one tuple remains in the result relation. Since at this point, we al-
  // ready know the candidate join is basically a semi join, we can further transform the join to a single predicate
  // node filtering the join column for the value of the remaining tuple's join attribute.
  visit_lqp(removable_subtree, [&removable_subtree, &rewrite_predicate,
                                &non_permanent_ucc_was_used](const auto& current_node) {
    if (current_node->type == LQPNodeType::Union) {
      return LQPVisitation::DoNotVisitInputs;
    }
    if (current_node->type != LQPNodeType::Predicate) {
      return LQPVisitation::VisitInputs;
    }

    // We need to get a predicate node with a BinaryPredicateExpression.
    const auto candidate = std::static_pointer_cast<PredicateNode>(current_node);
    const auto candidate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(candidate->predicate());
    if (!candidate_expression) {
      return LQPVisitation::VisitInputs;
    }

    // Only predicates in the form `column = value` are useful to our optimization. These conditions have the
    // potential (given filtered column is a UCC) to emit at most one result tuple.
    if (candidate_expression->predicate_condition != PredicateCondition::Equals) {
      return LQPVisitation::VisitInputs;
    }

    auto candidate_column_expression = std::shared_ptr<AbstractExpression>{};
    auto candidate_value_expression = std::shared_ptr<AbstractExpression>{};

    for (const auto& predicate_operand : candidate_expression->arguments) {
      if (predicate_operand->type == ExpressionType::LQPColumn) {
        candidate_column_expression = predicate_operand;
      } else if (predicate_operand->type == ExpressionType::Value) {
        candidate_value_expression = predicate_operand;
      }
    }

    // There should not be a case where we run into no column, but there may be a case where we have no value but
    // compare columns.
    if (!candidate_value_expression || !candidate_column_expression) {
      return LQPVisitation::VisitInputs;
    }

    // Check whether the referenced column is available for the subtree root node and unique. Checking whether the
    // column is unique on the current node is not sufficient. There could be unions or joins between the subtree
    // root and the current node, invalidating the unique column combination.
    if (!expression_evaluable_on_lqp(candidate_column_expression, *removable_subtree)) {
      return LQPVisitation::VisitInputs;
    }
    const auto matching_ucc = removable_subtree->get_matching_ucc({candidate_column_expression});
    if (!matching_ucc.has_value()) {
      return LQPVisitation::VisitInputs;
    }

    rewrite_predicate = candidate;
    non_permanent_ucc_was_used |= !matching_ucc->is_permanent();
    return LQPVisitation::DoNotVisitInputs;
  });

  if (rewrite_predicate) {
    rewritables.emplace_back(join_node, *prunable_side, rewrite_predicate);
  }
}

void perform_rewrite(const std::shared_ptr<JoinNode>& join_node, const LQPInputSide prunable_side,
                     const std::shared_ptr<PredicateNode>& rewrite_predicate) {
  if (prunable_side == LQPInputSide::Left) {
    join_node->set_left_input(join_node->right_input());
  }
  join_node->set_right_input(nullptr);

  // Get the join predicate, as we need to extract which column to filter on. We ensured before that there is exactly
  // one predicate for the current join.
  const auto& join_predicates = join_node->join_predicates();
  const auto& join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());

  Assert(join_predicate, "A join must have at least one BinaryPredicateExpression.");

  auto used_join_column = std::shared_ptr<AbstractExpression>{};
  auto projection_column = std::shared_ptr<AbstractExpression>{};

  if (expression_evaluable_on_lqp(join_predicate->left_operand(), *join_node->left_input())) {
    used_join_column = join_predicate->left_operand();
    projection_column = join_predicate->right_operand();
  } else if (expression_evaluable_on_lqp(join_predicate->right_operand(), *join_node->left_input())) {
    used_join_column = join_predicate->right_operand();
    projection_column = join_predicate->left_operand();
  }

  const auto projections = expression_vector(projection_column);
  const auto projection_node = ProjectionNode::make(projections, rewrite_predicate);
  const auto replacement_predicate_node =
      PredicateNode::make(equals_(used_join_column, lqp_subquery_(projection_node)));

  lqp_replace_node(join_node, replacement_predicate_node);
}

}  // namespace

namespace hyrise {

std::string JoinToPredicateRewriteRule::name() const {
  static const auto name = std::string{"JoinToPredicateRewriteRule"};
  return name;
}

IsCacheable JoinToPredicateRewriteRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // `rewritables finally contains all rewritable join nodes, their unused input side, and the predicates to be used for
  // the rewrites.
  auto rewritables = std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<PredicateNode>>>{};
  auto rule_was_applied_using_non_permanent_ucc = false;
  visit_lqp(lqp_root, [&](const auto& node) {
    if (node->type == LQPNodeType::Join) {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      gather_rewrite_info(join_node, rewritables, rule_was_applied_using_non_permanent_ucc);
    }
    return LQPVisitation::VisitInputs;
  });

  for (const auto& [join_node, prunable_side, rewrite_predicate] : rewritables) {
    perform_rewrite(join_node, prunable_side, rewrite_predicate);
  }

  return rule_was_applied_using_non_permanent_ucc ? IsCacheable::No : IsCacheable::Yes;
}

}  // namespace hyrise
