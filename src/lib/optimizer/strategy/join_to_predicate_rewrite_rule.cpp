#include "join_to_predicate_rewrite_rule.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace {

using namespace hyrise;                         // NOLINT(build/namespaces)
using namespace hyrise::expression_functional;  // NOLINT(build/namespaces)

bool qualifies_for_od_rewrite(const std::shared_ptr<PredicateNode>& predicate_node,
                              const std::shared_ptr<AbstractExpression>& candidate_column_expression,
                              const std::shared_ptr<AbstractExpression>& exchangeable_column_expression,
                              const std::shared_ptr<AbstractLQPNode>& join_node,
                              const std::shared_ptr<AbstractLQPNode>& removable_subtree) {
  // std::cout << *predicate_node << std::endl << *candidate_column_expression << std::endl << *exchangeable_column_expression << std::endl << *join_node << std::endl << *removable_subtree << std::endl;
  // Test that OD candidate_expression |-> exchangeable_column_expression holds.
  std::cout << __LINE__ << std::endl;
  for (const auto& od : predicate_node->order_dependencies()) {
    std::cout << od << std::endl;
  }

  std::cout << *predicate_node << std::endl;
  if (!predicate_node->has_matching_od({exchangeable_column_expression}, {candidate_column_expression})) {
    std::cout << __LINE__ << std::endl;
    return false;
  }

  // To ensure that all tuples between min(key) and max(key) match, there must be an IND without our predicate. We check
  // this by removing the predicate and look if the IND holds.
  const auto& left_input = predicate_node->left_input();
  lqp_remove_node(predicate_node);
  const auto rewritable = removable_subtree->has_matching_ind({exchangeable_column_expression}, *join_node);
  std::cout << __LINE__ << "  " << std::boolalpha << rewritable << std::endl;
  lqp_insert_node_above(left_input, predicate_node);
  return rewritable;
}

void perform_ucc_rewrite(const std::shared_ptr<JoinNode>& join_node, const LQPInputSide prunable_side,
                         const std::shared_ptr<AbstractLQPNode>& subquery_root,
                         const std::shared_ptr<AbstractExpression>& join_column,
                         const std::shared_ptr<AbstractExpression>& projection_column) {
  if (prunable_side == LQPInputSide::Left) {
    join_node->set_left_input(join_node->right_input());
  }
  join_node->set_right_input(nullptr);

  const auto projection_node = ProjectionNode::make(expression_vector(projection_column), subquery_root);
  const auto replacement_predicate_node = PredicateNode::make(equals_(join_column, lqp_subquery_(projection_node)));

  lqp_replace_node(join_node, replacement_predicate_node);
}

void perform_od_rewrite(const std::shared_ptr<JoinNode>& join_node, const LQPInputSide prunable_side,
                        const std::shared_ptr<AbstractLQPNode>& subquery_root,
                        const std::shared_ptr<AbstractExpression>& join_column,
                        const std::shared_ptr<AbstractExpression>& projection_column) {
  if (prunable_side == LQPInputSide::Left) {
    join_node->set_left_input(join_node->right_input());
  }
  join_node->set_right_input(nullptr);

  const auto aggregate_min = min_(projection_column);
  const auto aggregate_max = max_(projection_column);
  // clang-format off
  const auto aggregate_node =
  AggregateNode::make(expression_vector(), expression_vector(aggregate_min, aggregate_max),
    subquery_root);
  // clang-format on
  const auto projection_node_min = ProjectionNode::make(expression_vector(aggregate_min), aggregate_node);
  const auto projection_node_max = ProjectionNode::make(expression_vector(aggregate_max), aggregate_node);

  const auto replacement_predicate_node = PredicateNode::make(
      between_inclusive_(join_column, lqp_subquery_(projection_node_min), lqp_subquery_(projection_node_max)));

  lqp_replace_node(join_node, replacement_predicate_node);
}

void try_rewrite(const std::shared_ptr<JoinNode>& join_node) {
  const auto prunable_side = join_node->prunable_input_side();
  if (!prunable_side) {
    return;
  }

  // We don't handle the more complicated case of multiple join predicates. Rewriting in these cases would require
  // finding and combining multiple suitable predicates. We also only rewrite inner and semi joins. However,
  // AntiNullAsFalse joins could be rewritten to a scan with PredicateCondition::NotEquals. We cannot rewrite outer or
  // cross joins, as the results of the original and the rewritten query plan would not be equal.
  const auto& join_predicates = join_node->join_predicates();
  if (join_predicates.size() != 1 ||
      (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Semi)) {
    return;
  }

  const auto& join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());
  Assert(join_predicate, "A join must have at least one BinaryPredicateExpression.");

  auto removable_subtree = join_node->input(*prunable_side);
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

  // Check for uniqueness.
  if (!removable_subtree->has_matching_ucc({exchangeable_column_expression})) {
    return;
  }

  // Now, we look for a predicate that can potentially be used in a subquery to replace the join. If we find an equals
  // predicate that filters on a UCC, a maximum of one tuple remains in the result relation. Since at this point, we al-
  // ready know the candidate join is basically a semi join, we can further transform the join to a single predicate
  // node filtering the join column for the value of the remaining tuple's join attribute.
  auto performed_rewrite = false;
  visit_lqp(removable_subtree, [&](auto& current_node) {
    if (performed_rewrite || current_node->type == LQPNodeType::Union) {
      return LQPVisitation::DoNotVisitInputs;
    }
    if (current_node->type != LQPNodeType::Predicate) {
      return LQPVisitation::VisitInputs;
    }

    // We need to get a predicate node with a BinaryPredicateExpression.
    const auto candidate = std::static_pointer_cast<PredicateNode>(current_node);
    if (const auto& candidate_expression = std::dynamic_pointer_cast<BetweenExpression>(candidate->predicate())) {
      if (!expression_evaluable_on_lqp(candidate_expression->operand(), *removable_subtree) ||
          !qualifies_for_od_rewrite(candidate, candidate_expression->operand(), exchangeable_column_expression,
                                    join_node, removable_subtree)) {
        return LQPVisitation::VisitInputs;
      }

      perform_od_rewrite(join_node, *prunable_side, removable_subtree, used_join_column_expression,
                         exchangeable_column_expression);
      performed_rewrite = true;
      return LQPVisitation::DoNotVisitInputs;
    }

    const auto candidate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(candidate->predicate());
    if (!candidate_expression) {
      return LQPVisitation::VisitInputs;
    }

    // Only predicates in the form `column = value` are useful to our optimization. These conditions have the potential
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
    if (!candidate_value_expression || !candidate_column_expression) {
      return LQPVisitation::VisitInputs;
    }

    // Check whether the referenced column is available for the subtree root node and unique. Checking whether the
    // column is unique on the current node is not sufficient. There could be unions or joins in between the subtree
    // root and the current node, invalidating the unique column combination.
    if (!expression_evaluable_on_lqp(candidate_column_expression, *removable_subtree)) {
      return LQPVisitation::VisitInputs;
    }

    if (removable_subtree->has_matching_ucc({candidate_column_expression})) {
      perform_ucc_rewrite(join_node, *prunable_side, removable_subtree, used_join_column_expression,
                          exchangeable_column_expression);
      performed_rewrite = true;
      return LQPVisitation::DoNotVisitInputs;
    }

    std::cout << __LINE__ << std::endl;
    if (qualifies_for_od_rewrite(candidate, candidate_column_expression, exchangeable_column_expression, join_node,
                                 removable_subtree)) {
      std::cout << __LINE__ << std::endl;
      perform_od_rewrite(join_node, *prunable_side, removable_subtree, used_join_column_expression,
                         exchangeable_column_expression);
      performed_rewrite = true;
      return LQPVisitation::DoNotVisitInputs;
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace

namespace hyrise {

std::string JoinToPredicateRewriteRule::name() const {
  static const auto name = std::string{"JoinToPredicateRewriteRule"};
  return name;
}

void JoinToPredicateRewriteRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  visit_lqp(lqp_root, [&](const auto& node) {
    if (node->type == LQPNodeType::Join) {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      try_rewrite(join_node);
    }
    return LQPVisitation::VisitInputs;
  });
}

}  // namespace hyrise
