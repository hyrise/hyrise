#include "join_to_predicate_rewrite_rule.hpp"

#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "magic_enum.hpp"


namespace opossum {

std::string JoinToPredicateRewriteRule::name() const {
  static const auto name = std::string{"JoinToPredicateRewriteRule"};
  return name;
}

void JoinToPredicateRewriteRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  auto rewritable_nodes = std::vector<std::shared_ptr<JoinNode>>();
  auto removable_sides = std::vector<LQPInputSide>();
  auto valid_predicates = std::vector<std::shared_ptr<PredicateNode>>();

  visit_lqp(lqp_root, [&](const auto& node) {
    // std::cout << node->description() << std::endl;
    if (node->type == LQPNodeType::Join) {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      auto removable_side = join_node->get_unused_input();
      if ((removable_side) || (join_node->join_mode == JoinMode::Semi)) {
        // std::cout << "Has potential to be rewritten" << std::endl;

        std::shared_ptr<PredicateNode> valid_predicate = nullptr;
        const auto can_rewrite = _check_rewrite_validity(join_node, removable_side, valid_predicate);
        if (can_rewrite) {
          // std::cout << "Will rewrite for predicate: " << valid_predicate->description() << std::endl;
          rewritable_nodes.push_back(join_node);
          removable_sides.push_back(*removable_side);
          valid_predicates.push_back(valid_predicate);
        }
      }
    }
    return LQPVisitation::VisitInputs;
  });

  const auto n_rewritables = rewritable_nodes.size();
  for (auto index = size_t{0}; index < n_rewritables; ++index) {
    // std::cout << "  - Rewriting " << rewritable_nodes[index]->description() << std::endl;
    _perform_rewrite(rewritable_nodes[index], removable_sides[index], valid_predicates[index]);
  }

}

bool JoinToPredicateRewriteRule::_check_rewrite_validity(const std::shared_ptr<JoinNode>& join_node, std::optional<LQPInputSide>& removable_side, std::shared_ptr<PredicateNode>& valid_predicate) const {
  std::shared_ptr<AbstractLQPNode> removable_subtree = nullptr;
  
  if (removable_side) {
    removable_subtree = join_node->input(*removable_side);
  } else {
    // we know the join can only be a semi join in this case, where the right input is the one removed
    auto r = LQPInputSide::Right;
    removable_side = r;
    removable_subtree = join_node->right_input();
  }
  
  std::shared_ptr<BinaryPredicateExpression> join_predicate = nullptr;
  const auto& join_predicates = join_node->join_predicates();
  for (auto& predicate : join_predicates) {
    join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
    if (join_predicate) {
      break;
    }
  }

  DebugAssert(join_predicate, "A Join must have at least one BinaryPredicateExpression.");

  std::shared_ptr<AbstractExpression> exchangable_column_expr = nullptr;

  if (expression_evaluable_on_lqp(join_predicate->left_operand(), *removable_subtree)) {
    exchangable_column_expr = join_predicate->left_operand();
  } else if (expression_evaluable_on_lqp(join_predicate->right_operand(), *removable_subtree)) {
    exchangable_column_expr = join_predicate->right_operand();
  }

  DebugAssert(exchangable_column_expr, "Neither column of the join predicate could be evaluated on the removable input.");
  
  // Check for uniqueness
  auto testable_expressions = ExpressionUnorderedSet{};
  testable_expressions.insert(exchangable_column_expr);

  if (!removable_subtree->has_matching_unique_constraint(testable_expressions)) {
    // std::cout << "No unique constraint for join column found..." << exchangable_column_expr->description() << std::endl;
    return false;
  }

  // Now, we look for a predicate that can be used inside the substituting table scan node.
  visit_lqp(removable_subtree, [&exchangable_column_expr, &removable_subtree, &valid_predicate](auto& current_node) {
    if (current_node->type != LQPNodeType::Predicate) return LQPVisitation::VisitInputs;

    const auto candidate = std::static_pointer_cast<PredicateNode>(current_node);
    const auto candidate_exp = std::dynamic_pointer_cast<BinaryPredicateExpression>(candidate->predicate());

    if (!candidate_exp) {
      return LQPVisitation::VisitInputs;
    }

    DebugAssert(candidate_exp, "Need to get a predicate with a BinaryPredicateExpression.");

    // Only predicates that filter by the equals condition on a column with a constant value are of use to our optimization, because these conditions have the potential (given that the filtered column is a UCC) to single out a maximum of one result tuple.
    if (candidate_exp->predicate_condition != PredicateCondition::Equals) return LQPVisitation::VisitInputs;

    auto candidate_column_expr = std::dynamic_pointer_cast<LQPColumnExpression>(candidate_exp->left_operand());
    std::shared_ptr<ValueExpression> candidate_value_expr = nullptr;
    if (candidate_column_expr) {
      candidate_value_expr = std::dynamic_pointer_cast<ValueExpression>(candidate_exp->right_operand());
    } else {
      candidate_column_expr = std::dynamic_pointer_cast<LQPColumnExpression>(candidate_exp->right_operand());
      candidate_value_expr = std::dynamic_pointer_cast<ValueExpression>(candidate_exp->left_operand());
    }

    // should not have a case where we run into no column, but may be a case where we have no value but compare columns
    if (!candidate_column_expr || !candidate_value_expr) {
      return LQPVisitation::VisitInputs;
    }

    // in case Predicate column expr != join column expression, check whether the column referenced is still available in join
    if (!expression_evaluable_on_lqp(candidate_column_expr, *removable_subtree)) {
      return LQPVisitation::VisitInputs;
    }

    // Check for uniqueness
    auto testable_expressions = ExpressionUnorderedSet{};
    testable_expressions.insert(candidate_column_expr);

    if (!removable_subtree->has_matching_unique_constraint(testable_expressions)) {
      // std::cout << "No unique constraint matching the predicate column found..." << candidate_column_expr->description(AbstractExpression::DescriptionMode::Detailed) << std::endl;
      return LQPVisitation::VisitInputs;
    }

    valid_predicate = candidate;
    return LQPVisitation::DoNotVisitInputs;
  });
  
  if (!valid_predicate) {
    return false;
  }

  return true;
}

void JoinToPredicateRewriteRule::_perform_rewrite(const std::shared_ptr<JoinNode>& join_node, const LQPInputSide& removable_side, const std::shared_ptr<PredicateNode>& valid_predicate) const  {
  const auto param_ids = std::vector<ParameterID>{};
  const auto param_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};

  // std::cout << "Rewrite of Node: " << join_node->description() << std::endl;

  if (removable_side == LQPInputSide::Left) {
    join_node->set_left_input(join_node->right_input());
  }
  join_node->set_right_input(nullptr);

  // get the join predicate, as we need to extract which column to filter on
  std::shared_ptr<BinaryPredicateExpression> join_predicate = nullptr;
  const auto& join_predicates = join_node->join_predicates();
  for (auto& predicate : join_predicates) {
    join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
    if (join_predicate) {
      break;
    }
  }

  DebugAssert(join_predicate, "A Join must have at least one BinaryPredicateExpression.");

  std::shared_ptr<AbstractExpression> used_join_column = nullptr;
  std::shared_ptr<AbstractExpression> projection_column = nullptr;

  if (expression_evaluable_on_lqp(join_predicate->left_operand(), *join_node->left_input())) {
    used_join_column = join_predicate->left_operand();
    projection_column = join_predicate->right_operand();
  } else if (expression_evaluable_on_lqp(join_predicate->right_operand(), *join_node->left_input())) {
    used_join_column = join_predicate->right_operand();
    projection_column = join_predicate->left_operand();
  }

  auto projections = std::vector<std::shared_ptr<AbstractExpression>>{};
  projections.push_back(projection_column);

  auto projection_node = std::make_shared<ProjectionNode>(projections);
  projection_node->set_left_input(valid_predicate);

  auto replacement_predicate_node = std::make_shared<PredicateNode>(
    std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::Equals,
      used_join_column,
      std::make_shared<LQPSubqueryExpression>(projection_node, param_ids, param_expressions)
    )
  );

  lqp_replace_node(join_node, replacement_predicate_node);
}

}  // namespace opossum
