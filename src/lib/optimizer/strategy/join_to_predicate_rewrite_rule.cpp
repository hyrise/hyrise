#include "join_to_predicate_rewrite_rule.hpp"

#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"


namespace opossum {

std::string JoinToPredicateRewriteRule::name() const {
  static const auto name = std::string{"JoinToPredicateRewriteRule"};
  return name;
}

/*
There are multiple options that don't work in different ways:

1. (not currently active): running _perform_rewrite within the visit_lqp method as-is. This will abort the iteration as far as I can tell, so only one rewrite would happen
    other than that though, this works and spits out a seemingly correct LQP with the SubqueryExpression-based PredicateNode having replaced the join
2. (not currently active): running _preform_rewrite within the visit_lqp method but running the visit_lqp inside a while loop 
    the idea here is that we only abort running the while loop (so re-running visit_lqp) once we have visited the full tree once without wanting to rewrite anything
    this does work in principle, iterating through both the LQP and also the newly inserted SubqueryExpression tree
    However, this causes an unexplainable segmentation fault later on in the ChunkPruningRule, where a hashing function is failing
3. (currently active): while visiting the lqp with the visit_lqp method, do not modify it, but instead write down which nodes are rewritable (join node pointer), their removable side and the predicate to use in the replacing subtree
    This seems like a good idea in theory, but fails for the following query as the rewrite for some reason is told to attach the join node output to the right input of the AggregateNode, which is not allowed
    SELECT c_name, SUM(o_totalprice) FROM customer, nation, orders WHERE n_name='GERMANY' AND n_nationkey=c_nationkey AND o_custkey=c_custkey AND o_custkey=370 GROUP BY c_name;
    An attempt to work around this by simply statically setting the input to left (which would not be feasable in production) resulted in a segfault in the PredicateReorderingRule
*/

void JoinToPredicateRewriteRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  auto rewritable_nodes = std::vector<std::shared_ptr<JoinNode>>();
  auto removable_sides = std::vector<std::shared_ptr<LQPInputSide>>();
  auto valid_predicates = std::vector<std::shared_ptr<PredicateNode>>();
/*
  bool visited_full_tree = false;
  
  while (!visited_full_tree) {
    bool early_abort = false;
*/
  visit_lqp(lqp_root, [&](const auto& node) {
    std::cout << node->description() << std::endl;
    if (node->type == LQPNodeType::Join) {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      const auto removable_side = join_node->get_unused_input();
      if ((removable_side) || (join_node->join_mode == JoinMode::Semi)) {
        std::cout << "Has potential to be rewritten" << std::endl;

        std::shared_ptr<PredicateNode> valid_predicate = nullptr;
        const auto can_rewrite = _check_rewrite_validity(join_node, removable_side, valid_predicate);
        if (can_rewrite) {
          std::cout << "Will rewrite for predicate: " << valid_predicate->description() << std::endl;
          rewritable_nodes.push_back(join_node);
          removable_sides.push_back(removable_side);
          valid_predicates.push_back(valid_predicate);
          // _perform_rewrite(join_node, removable_side, valid_predicate);
          // early_abort = true;
        }
      }
    }
    return LQPVisitation::VisitInputs;
  });
/*
    visited_full_tree = !early_abort;
    if (visited_full_tree) {
      std::cout << "Finished..." << std::endl;
    } else {
      std::cout << "Doing another pass..." << std::endl;
    }
  }
*/
  const auto n_rewritables = rewritable_nodes.size();
  for (auto index = size_t{0}; index < n_rewritables; ++index) {
    _perform_rewrite(rewritable_nodes[index], removable_sides[index], valid_predicates[index]);
  }

}

bool JoinToPredicateRewriteRule::_check_rewrite_validity(const std::shared_ptr<JoinNode>& join_node, std::shared_ptr<LQPInputSide> removable_side, std::shared_ptr<PredicateNode>& valid_predicate) const {
  std::shared_ptr<AbstractLQPNode> removable_subtree = nullptr;
  
  if (removable_side) {
    removable_subtree = join_node->input(*removable_side);
  } else {
    // we know the join can only be a semi join in this case, where the right input is the one removed
    auto r = LQPInputSide::Right;
    removable_side = std::shared_ptr<LQPInputSide>(&r);
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
    std::cout << "No unique constraint matching the given column found..." << std::endl;
    return false;
  }

  // Now, we look for a predicate that can be used inside the substituting table scan node.
  visit_lqp(removable_subtree, [&exchangable_column_expr, &removable_subtree, &valid_predicate](auto& current_node) {
    if (current_node->type != LQPNodeType::Predicate) return LQPVisitation::VisitInputs;

    const auto candidate = std::static_pointer_cast<PredicateNode>(current_node);
    const auto candidate_exp = std::dynamic_pointer_cast<BinaryPredicateExpression>(candidate->predicate());

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

    valid_predicate = candidate;
    return LQPVisitation::DoNotVisitInputs;
  });
  
  if (!valid_predicate) {
    return false;
  }

  return true;
}

void JoinToPredicateRewriteRule::_perform_rewrite(const std::shared_ptr<JoinNode>& join_node, const std::shared_ptr<LQPInputSide> removable_side, const std::shared_ptr<PredicateNode>& valid_predicate) const  {
  const auto param_ids = std::vector<ParameterID>{};
  const auto param_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};

  std::cout << "Rewrite of Node: " << join_node->description() << std::endl;

  const auto node_outputs = join_node->outputs();
  const auto input_sides = join_node->get_input_sides();
  const auto used_input = (*removable_side == LQPInputSide::Left) ? join_node->right_input() : join_node->left_input();

  // get the join predicate, as we need to extract which column to filter on
  const auto predicate_exp = std::static_pointer_cast<BinaryPredicateExpression>(valid_predicate->predicate());
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

  if (expression_evaluable_on_lqp(join_predicate->left_operand(), *used_input)) {
    used_join_column = join_predicate->left_operand();
  } else if (expression_evaluable_on_lqp(join_predicate->right_operand(), *used_input)) {
    used_join_column = join_predicate->right_operand();
  }

  auto candidate_column_expr = std::dynamic_pointer_cast<LQPColumnExpression>(predicate_exp->left_operand());
  std::shared_ptr<ValueExpression> candidate_value_expr = nullptr;
  if (candidate_column_expr) {
    candidate_value_expr = std::dynamic_pointer_cast<ValueExpression>(predicate_exp->right_operand());
  } else {
    candidate_column_expr = std::dynamic_pointer_cast<LQPColumnExpression>(predicate_exp->right_operand());
    candidate_value_expr = std::dynamic_pointer_cast<ValueExpression>(predicate_exp->left_operand());
  }


  auto projections = std::vector<std::shared_ptr<AbstractExpression>>{};
  projections.push_back(candidate_column_expr);

  auto projection_node = std::make_shared<ProjectionNode>(projections);
  projection_node->set_left_input(valid_predicate);

  auto replacement_predicate_node = std::make_shared<PredicateNode>(
    std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::Equals,
      used_join_column,
      std::make_shared<LQPSubqueryExpression>(projection_node, param_ids, param_expressions)
    )
  );

  // tie replacement node with thie join node's used input
  replacement_predicate_node->set_left_input(used_input);

  // tie replacement node with the join node's outputs
  // TODO:
  // For query SELECT c_name, SUM(o_totalprice) FROM customer, nation, orders WHERE n_name='GERMANY' AND n_nationkey=c_nationkey AND o_custkey=c_custkey AND o_custkey=370 GROUP BY c_name
  // There is one output - aggregateNode - but the input_sides show "right" -> something is wrong but I don't know what
  for (size_t output_idx = 0; output_idx < node_outputs.size(); ++output_idx) {
    std::cout << node_outputs[output_idx]->description() << std::endl;
    node_outputs[output_idx]->set_input(input_sides[output_idx], replacement_predicate_node);
  }

  // remove join node from LQP tree
  join_node->set_left_input(nullptr);
  join_node->set_right_input(nullptr);
}

}  // namespace opossum
