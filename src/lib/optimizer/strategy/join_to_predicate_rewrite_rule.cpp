#include "join_to_predicate_rewrite_rule.hpp"

#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"


namespace opossum {

/*
void try_join_to_local_predicate_rewrite(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) {

  const auto& join_node = std::dynamic_pointer_cast<JoinNode>(node);
  // A join that isn't a semi join or can't be transformed to a semi join doesn't fulfill the requirements for this optimization.
  // We are expecting here that the join to semi join optimization strategy has already been run. This kind of dependency between optimization strategies is potentially problematic, so we may have to do a rewrite to handle all kinds of joins.
  // We also noticed that apperently the join-to-semi-join strategy though applicable is not performed before a LIMIT node.
  if (join_node->join_mode == JoinMode::Semi) {
    auto left_table = std::shared_ptr<StoredTableNode>{};
    auto right_table = std::shared_ptr<StoredTableNode>{};
    
    visit_lqp(node->left_input(), [&left_table](auto& node) {
      if (node->type == LQPNodeType::StoredTable) {
        left_table = std::dynamic_pointer_cast<StoredTableNode>(node);
        return LQPVisitation::DoNotVisitInputs;
      }
      
      return LQPVisitation::VisitInputs;
    });

    visit_lqp(node->right_input(), [&right_table](auto& node) {
      if (node->type == LQPNodeType::StoredTable) {
        right_table = std::dynamic_pointer_cast<StoredTableNode>(node);
        return LQPVisitation::DoNotVisitInputs;
      }
      
      return LQPVisitation::VisitInputs;
    });

    std::cout << left_table->description() << std::endl;
    std::cout << right_table->description() << std::endl;

    const auto& join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_node->join_predicates()[0]);

    std::cout << expression_evaluable_on_lqp(join_predicate->left_operand(), *node->left_input()) << std::endl;
    std::cout << expression_evaluable_on_lqp(join_predicate->left_operand(), *node->right_input()) << std::endl;

    std::cout << expression_evaluable_on_lqp(join_predicate->right_operand(), *node->left_input()) << std::endl;
    std::cout << expression_evaluable_on_lqp(join_predicate->right_operand(), *node->right_input()) << std::endl;

    const auto& stored_table_node = std::make_shared<StoredTableNode>(right_table->table_name);

    auto propagatable_predicate = std::shared_ptr<PredicateNode>{};

    // Now, we look for a predicate that can be used inside the substituting table scan node.
    visit_lqp(node->right_input(), [&node, &propagatable_predicate](auto& current_node) {
      if (current_node->type != LQPNodeType::Predicate) {
        return LQPVisitation::VisitInputs;
      }

      const auto& candidate = std::dynamic_pointer_cast<PredicateNode>(current_node);
      const auto& candidate_exp = std::dynamic_pointer_cast<BinaryPredicateExpression>(candidate->predicate());

      std::cout << candidate->description() << std::endl;

      // Only predicates that filter by the equals condition on a column with a constant value are of use to our optimization, because these conditions have the potential (given that the filtered column is a UCC) to single out a maximum of one result tuple.
      if (candidate_exp->predicate_condition != PredicateCondition::Equals || !std::dynamic_pointer_cast<LQPColumnExpression>(candidate_exp->left_operand()) || !std::dynamic_pointer_cast<ValueExpression>(candidate_exp->right_operand())) {
        return LQPVisitation::VisitInputs;
      }

      const auto& filtered_column_exp = std::dynamic_pointer_cast<LQPColumnExpression>(candidate_exp->left_operand());
      std::cout << filtered_column_exp->description(AbstractExpression::DescriptionMode::Detailed) << std::endl;

      // If the attribute that is filtered on is projected away (through projections, semi joins) on the way to the join node, we can't use the predicate directly in place of the join node.
      if (!expression_evaluable_on_lqp(filtered_column_exp, *node->right_input())) {
        return LQPVisitation::VisitInputs;
      }

      // Check for uniqueness
      auto testable_expressions = ExpressionUnorderedSet{};
      testable_expressions.insert(filtered_column_exp);

      if (!current_node->left_input()->has_matching_unique_constraint(testable_expressions)) {
        // This line is commented out here for testing purposes, so that the UCC discovery plugin doesn't have to run every time before the optimization.
        // return LQPVisitation::VisitInputs;
      }

      propagatable_predicate = candidate;

      // It is potentially wrong to 
      return LQPVisitation::DoNotVisitInputs;
    });

    std::cout << "Description: " << propagatable_predicate->description() << std::endl;

    // const auto& predicate_node = std::make_shared<PredicateNode>(std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, std::make_shared<LQPColumnExpression>(stored_table_node, ColumnID{1}), std::make_shared<ValueExpression>("BRAZIL")));
    // predicate_node->set_left_input(stored_table_node);

    // const auto predicate_node = std::make_shared<PredicateNode>(propagatable_predicate->predicate());
    // predicate_node->set_left_input(propagatable_predicate->left_input());

    propagatable_predicate->clear_outputs();

    auto projections = std::vector<std::shared_ptr<AbstractExpression>>{};
    projections.push_back(std::make_shared<LQPColumnExpression>(stored_table_node, ColumnID{0}));

    const auto& query_subtree = std::make_shared<ProjectionNode>(projections);
    query_subtree->set_left_input(propagatable_predicate);

    const auto param_ids = std::vector<ParameterID>{};
    const auto param_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
    
    const auto& new_predicate_node = std::make_shared<PredicateNode>(std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, std::make_shared<LQPColumnExpression>(left_table, ColumnID{3}), std::make_shared<LQPSubqueryExpression>(query_subtree, param_ids, param_expressions)));
    new_predicate_node->set_left_input(node->left_input());

    for (const auto& output: node->outputs()) {
      if (node->get_input_side(output) == LQPInputSide::Left) {
        output->set_left_input(new_predicate_node);
      } else {
        output->set_right_input(new_predicate_node);
      }
    }

    node->left_input()->remove_output(node);
    // The line below seems to trigger an unwanted situation in which the number of outputs of one node is inconsistent with the outputs_visited_by_node datastructe.
    // node->right_input()->remove_output(node);

    node->clear_outputs();
    // std::cout << node->description() << std::endl;
  }
}
*/

std::string JoinToPredicateRewriteRule::name() const {
  static const auto name = std::string{"JoinToPredicateRewriteRule"};
  return name;
}

void JoinToPredicateRewriteRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  visit_lqp(lqp_root, [&](const auto& node) {
    if (node->type == LQPNodeType::Join) {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      const auto removable_side = join_node->get_unused_input();
      std::cout << join_node->description() << std::endl;
      if ((removable_side) || (join_node->join_mode == JoinMode::Semi)) {
        std::cout << "Has potential to be rewritten" << std::endl;
        std::cout << removable_side << std::endl;
        const auto can_rewrite = _check_rewrite_validity(join_node, removable_side);
        if (can_rewrite) {
          _perform_rewrite(join_node, removable_side);
        }
      }
    }

    return LQPVisitation::VisitInputs;
  });
}

bool JoinToPredicateRewriteRule::_check_rewrite_validity(const std::shared_ptr<JoinNode>& join_node, const std::shared_ptr<LQPInputSide> removable_side) const {
  std::shared_ptr<AbstractLQPNode> removable_subtree = nullptr;
  
  if (removable_side) {
    removable_subtree = join_node->input(*removable_side);
  } else {
    // we know the join can only be a semi join in this case, where the right input is the one removed
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

  std::cout << expression_evaluable_on_lqp(join_predicate->left_operand(), *removable_subtree) << std::endl;
  std::cout << expression_evaluable_on_lqp(join_predicate->right_operand(), *removable_subtree) << std::endl;

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


  // we fail with a logic error here
  // the error happens in abstract_lqp_node.cpp line 267
  // it seems to be related to the expression passed to be tested for the unique constraint
  if (!join_node->has_matching_unique_constraint(testable_expressions)) {
    return false;
  }

  std::shared_ptr<PredicateNode> valid_predicate = nullptr;

  // Now, we look for a predicate that can be used inside the substituting table scan node.
  visit_lqp(removable_subtree, [&exchangable_column_expr, &removable_subtree](auto& current_node) {
    if (current_node->type != LQPNodeType::Predicate) return LQPVisitation::VisitInputs;

    const auto candidate = std::static_pointer_cast<PredicateNode>(current_node);
    const auto candidate_exp = std::dynamic_pointer_cast<BinaryPredicateExpression>(candidate->predicate());

    DebugAssert(candidate_exp, "Need to get a predicate with a BinaryPredicateExpression.");

    std::cout << candidate->description() << std::endl;

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

    return LQPVisitation::VisitInputs;

  });



  /*
  * check whether exchangable_column_expr is UCC
  * Find PredicateNode in removable subtree
    -> if None, can't optimize
  * On found PredicateNode
    -> check whether PredicateNode ColumnExpression == exchangable_column_expr
      -> if true, replace exchangable_column_expr with Projection(exchangable_column_expr) + PredicateNode and StoredTableNode
      -> otherwise, check if PredicateNode ColumnExpression is also UCC
        -> if true, replace exchangable_column_expr with Projection(exchangable_column_expr) + PredicateNode and StoredTableNode
        -> otherwise, can't replace
  */
  
  return false;
}
void JoinToPredicateRewriteRule::_perform_rewrite(const std::shared_ptr<JoinNode>& join_node, const std::shared_ptr<LQPInputSide> removable_side) const  {

}

}  // namespace opossum
