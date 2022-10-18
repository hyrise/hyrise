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

using namespace hyrise;                         // NOLINT
using namespace hyrise::expression_functional;  // NOLINT

void gather_rewrite_info(
    const std::shared_ptr<JoinNode>& join_node,
    std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<PredicateNode>>>& rewritables) {
  const auto prunable_side = join_node->prunable_input_side();
  if (!prunable_side) {
    return;
  }

  auto removable_subtree = std::shared_ptr<AbstractLQPNode>{};
  auto valid_predicate = std::shared_ptr<PredicateNode>{};

  removable_subtree = join_node->input(*prunable_side);

  // We don't handle the more complicated case of multiple join predicates. Rewriting in these cases would require
  // finding and combining multiple suitable predicates. We also only rewrite inner ans semi joins. However,
  // AntiNullAsFalse joins could be rewritten to a scan with PredicateCondition::NotEquals. We cannot rewrite outer or
  // cross joins, as the results of the orignal and thhe rewritten query plan would not be equal.
  if (join_node->join_predicates().size() != 1 ||
      (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Semi)) {
    return;
  }

  const auto& join_predicates = join_node->join_predicates();
  const auto& join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());
  Assert(join_predicate, "A join must have at least one BinaryPredicateExpression.");

  auto exchangable_column_expression = std::shared_ptr<AbstractExpression>{};

  if (expression_evaluable_on_lqp(join_predicate->left_operand(), *removable_subtree)) {
    exchangable_column_expression = join_predicate->left_operand();
  } else if (expression_evaluable_on_lqp(join_predicate->right_operand(), *removable_subtree)) {
    exchangable_column_expression = join_predicate->right_operand();
  }

  Assert(exchangable_column_expression,
         "Neither column of the join predicate could be evaluated on the removable input.");

  // Check for uniqueness.
  if (!removable_subtree->has_matching_unique_constraint({exchangable_column_expression})) {
    return;
  }

  // Now, we look for a predicate that can be a subquery for a potential predicate that replaces the join.
  // If we find an equals predicate that filters on a UCC, a maximum of one tuple is remains in the result relation.
  // Since at this point, we already know the candidate join is basically a semi-join, we can further transform the
  // join to a single predicate node filtering the join column for the value of the remaining tuple's join attribute.
  visit_lqp(removable_subtree, [&removable_subtree, &valid_predicate](auto& current_node) {
    if (current_node->type == LQPNodeType::Union) {
      return LQPVisitation::DoNotVisitInputs;
    }
    if (current_node->type != LQPNodeType::Predicate) {
      return LQPVisitation::VisitInputs;
    }

    const auto candidate = std::static_pointer_cast<PredicateNode>(current_node);
    const auto candidate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(candidate->predicate());

    if (!candidate_expression) {
      return LQPVisitation::VisitInputs;
    }

    Assert(candidate_expression, "Need to get a predicate with a BinaryPredicateExpression.");

    // Only predicates in the form `column = value` are of use to our optimization. These conditions have the potential
    // (given filtered column is a UCC) to emit at most one result tuple.
    if (candidate_expression->predicate_condition != PredicateCondition::Equals)
      return LQPVisitation::VisitInputs;

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
    if (!candidate_column_expression || !candidate_value_expression) {
      return LQPVisitation::VisitInputs;
    }

    // If the predicate column expression is not equals to the join column expression, check whether the column
    // referenced is still available in the join.
    if (!expression_evaluable_on_lqp(candidate_column_expression, *removable_subtree)) {
      return LQPVisitation::VisitInputs;
    }

    // Check for uniqueness.
    if (!removable_subtree->has_matching_unique_constraint({candidate_column_expression})) {
      return LQPVisitation::VisitInputs;
    }

    valid_predicate = candidate;
    return LQPVisitation::DoNotVisitInputs;
  });

  if (valid_predicate) {
    rewritables.emplace_back(join_node, *prunable_side, valid_predicate);
  }
}

void perform_rewrite(const std::shared_ptr<JoinNode>& join_node, const LQPInputSide prunable_side,
                     const std::shared_ptr<PredicateNode>& valid_predicate) {
  if (prunable_side == LQPInputSide::Left) {
    join_node->set_left_input(join_node->right_input());
  }
  join_node->set_right_input(nullptr);

  // Get the join predicate, as we need to extract which column to filter on. We ensured before that there is exactly
  // one predicate for the current join.
  const auto& join_predicates = join_node->join_predicates();
  const auto& join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());

  Assert(join_predicate, "A Join must have at least one BinaryPredicateExpression.");

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
  const auto projection_node = ProjectionNode::make(projections, valid_predicate);
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

void JoinToPredicateRewriteRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // rewritables finally contains all rewritable join nodes, there unused input side and the predicate to be used for
  // the rewrite.
  auto rewritables = std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<PredicateNode>>>{};

  visit_lqp(lqp_root, [&](const auto& node) {
    if (node->type == LQPNodeType::Join) {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      gather_rewrite_info(join_node, rewritables);
    }
    return LQPVisitation::VisitInputs;
  });

  for (const auto& rewrite_candidate : rewritables) {
    perform_rewrite(std::get<0>(rewrite_candidate), std::get<1>(rewrite_candidate), std::get<2>(rewrite_candidate));
  }
}

}  // namespace hyrise
