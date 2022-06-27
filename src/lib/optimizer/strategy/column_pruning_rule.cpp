#include "column_pruning_rule.hpp"

#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"

namespace {

using namespace opossum;                         // NOLINT
using namespace opossum::expression_functional;  // NOLINT

void gather_expressions_not_computed_by_expression_evaluator(
    const std::shared_ptr<AbstractExpression>& expression,
    const std::vector<std::shared_ptr<AbstractExpression>>& input_expressions,
    ExpressionUnorderedSet& required_expressions, const bool top_level = true) {
  // Top-level expressions are those that are (part of) the ExpressionEvaluator's final result. For example, for an
  // ExpressionEvaluator producing (a + b) + c, the entire expression is a top-level expression. It is the consumer's
  // job to mark it as required. (a + b) however, is required by the ExpressionEvaluator and will be added to
  // required_expressions, as it is not a top-level expression.

  // If an expression that is not a top-level expression is already an input, we require it
  if (std::find_if(input_expressions.begin(), input_expressions.end(),
                   [&expression](const auto& other) { return *expression == *other; }) != input_expressions.end()) {
    if (!top_level) required_expressions.emplace(expression);
    return;
  }

  if (expression->type == ExpressionType::Aggregate || expression->type == ExpressionType::LQPColumn) {
    // Aggregates and LQPColumns are not calculated by the ExpressionEvaluator and are thus required to be part of the
    // input.
    required_expressions.emplace(expression);
    return;
  }

  for (const auto& argument : expression->arguments) {
    gather_expressions_not_computed_by_expression_evaluator(argument, input_expressions, required_expressions, false);
  }
}

ExpressionUnorderedSet gather_locally_required_expressions(
    const std::shared_ptr<AbstractLQPNode>& node, const ExpressionUnorderedSet& expressions_required_by_consumers) {
  // Gathers all expressions required by THIS node, i.e., expressions needed by the node to do its job. For example, a
  // PredicateNode `a < 3` requires the LQPColumn a.
  auto locally_required_expressions = ExpressionUnorderedSet{};

  switch (node->type) {
    // For the vast majority of node types, AbstractLQPNode::node_expression holds all expressions required by this
    // node.
    case LQPNodeType::Alias:
    case LQPNodeType::CreatePreparedPlan:
    case LQPNodeType::CreateView:
    case LQPNodeType::DropView:
    case LQPNodeType::DropTable:
    case LQPNodeType::DummyTable:
    case LQPNodeType::Import:
    case LQPNodeType::Limit:
    case LQPNodeType::Root:
    case LQPNodeType::Sort:
    case LQPNodeType::StaticTable:
    case LQPNodeType::StoredTable:
    case LQPNodeType::Validate:
    case LQPNodeType::Mock: {
      for (const auto& expression : node->node_expressions) {
        locally_required_expressions.emplace(expression);
      }
    } break;

    // For aggregate nodes, we need the group by columns and the arguments to the aggregate functions
    case LQPNodeType::Aggregate: {
      const auto& aggregate_node = static_cast<AggregateNode&>(*node);
      const auto& node_expressions = node->node_expressions;

      for (auto expression_idx = ColumnID{0}; expression_idx < node_expressions.size(); ++expression_idx) {
        const auto& expression = node_expressions[expression_idx];
        // The AggregateNode's node_expressions contain both the group_by- and the aggregate_expressions in that order,
        // separated by aggregate_expressions_begin_idx.
        if (expression_idx < aggregate_node.aggregate_expressions_begin_idx) {
          // All group_by-expressions are required
          locally_required_expressions.emplace(expression);
        } else {
          // We need the arguments of all aggregate functions
          DebugAssert(expression->type == ExpressionType::Aggregate, "Expected AggregateExpression");
          if (!AggregateExpression::is_count_star(*expression)) {
            locally_required_expressions.emplace(expression->arguments[0]);
          } else {
            /**
             * COUNT(*) is an edge case: The aggregate function contains a pseudo column expression with an
             * INVALID_COLUMN_ID. We cannot require the latter from other nodes. However, in the end, we have to
             * ensure that the AggregateNode requires at least one expression from other nodes.
             * For
             *  a) grouped COUNT(*) aggregates, this is guaranteed by the group-by column(s).
             *  b) ungrouped COUNT(*) aggregates, it may be guaranteed by other aggregate functions. But, if COUNT(*)
             *     is the only type of aggregate function, we simply require the first output expression from the
             *     left input node.
             */
            if (!locally_required_expressions.empty() || expression_idx < node_expressions.size() - 1) continue;
            locally_required_expressions.emplace(node->left_input()->output_expressions().at(0));
          }
        }
      }
    } break;

    // For ProjectionNodes, collect all expressions that
    //   (1) were already computed and are re-used as arguments in this projection
    //   (2) cannot be computed (i.e., Aggregate and LQPColumn inputs)
    // PredicateNodes have the same requirements - if they have their own implementation, they require all columns to
    // be already computed; if they use the ExpressionEvaluator the columns should at least be computable.
    case LQPNodeType::Predicate:
    case LQPNodeType::Projection: {
      const auto& input_expressions = node->left_input()->output_expressions();
      for (const auto& expression : node->node_expressions) {
        if (node->type == LQPNodeType::Projection && !expressions_required_by_consumers.contains(expression)) {
          // An expression produced by a ProjectionNode that is not required by anyone upstream is useless. We should
          // not collect the expressions required for calculating that useless expression.
          continue;
        }

        gather_expressions_not_computed_by_expression_evaluator(expression, input_expressions,
                                                                locally_required_expressions);
      }
    } break;

    // For Joins, collect the expressions used on the left and right sides of the join expressions
    case LQPNodeType::Join: {
      const auto& join_node = static_cast<JoinNode&>(*node);
      for (const auto& predicate : join_node.join_predicates()) {
        DebugAssert(predicate->type == ExpressionType::Predicate && predicate->arguments.size() == 2,
                    "Expected binary predicate for join");
        locally_required_expressions.emplace(predicate->arguments[0]);
        locally_required_expressions.emplace(predicate->arguments[1]);
      }
    } break;

    case LQPNodeType::Union: {
      const auto& union_node = static_cast<const UnionNode&>(*node);
      switch (union_node.set_operation_mode) {
        case SetOperationMode::Positions: {
          // UnionNode does not require any expressions itself for the Positions mode. As Positions by definition
          // operates on the same table left and right, we simply require the same input expressions from both sides.
        } break;

        case SetOperationMode::All: {
          // Similarly, if the two input tables are only glued together, the UnionNode itself does not require any
          // expressions. Currently, this mode is used to merge the result of two mutually exclusive or conditions (see
          // PredicateSplitUpRule). Once we have a union operator that merges data from different tables, we have to
          // look into this more deeply.
          Assert(union_node.left_input()->output_expressions() == union_node.right_input()->output_expressions(),
                 "Can only handle SetOperationMode::All if both inputs have the same expressions");
        } break;

        case SetOperationMode::Unique: {
          // This probably needs all expressions, as all of them are used to establish uniqueness
          Fail("SetOperationMode::Unique is not supported yet");
        }
      }
    } break;

    case LQPNodeType::Intersect:
    case LQPNodeType::Except: {
      Fail("Intersect and Except are not supported yet");
      // Not sure what needs to happen here. That partially depends on how intersect and except are finally implemented.
    } break;

    // No pruning of the input columns for these nodes as they need them all.
    case LQPNodeType::CreateTable:
    case LQPNodeType::Delete:
    case LQPNodeType::Insert:
    case LQPNodeType::Export:
    case LQPNodeType::Update:
    case LQPNodeType::ChangeMetaTable: {
      const auto& left_input_expressions = node->left_input()->output_expressions();
      locally_required_expressions.insert(left_input_expressions.begin(), left_input_expressions.end());

      if (node->right_input()) {
        const auto& right_input_expressions = node->right_input()->output_expressions();
        locally_required_expressions.insert(right_input_expressions.begin(), right_input_expressions.end());
      }
    } break;
  }

  return locally_required_expressions;
}

void recursively_gather_required_expressions(
    const std::shared_ptr<AbstractLQPNode>& node,
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node,
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t>& outputs_visited_by_node) {
  auto& required_expressions = required_expressions_by_node[node];
  const auto locally_required_expressions = gather_locally_required_expressions(node, required_expressions);
  required_expressions.insert(locally_required_expressions.begin(), locally_required_expressions.end());

  // We only continue with node's inputs once we have visited all paths above node. We check this by counting the
  // number of the node's outputs that have already been visited. Once we reach the output count, we can continue.
  if (node->type != LQPNodeType::Root) ++outputs_visited_by_node[node];
  if (outputs_visited_by_node[node] < node->output_count()) return;

  // Once all nodes that may require columns from this node (i.e., this node's outputs) have been visited, we can
  // recurse into this node's inputs.
  for (const auto& input : {node->left_input(), node->right_input()}) {
    if (!input) continue;

    // Make sure the entry in required_expressions_by_node exists, then insert all expressions that the current node
    // needs
    auto& required_expressions_for_input = required_expressions_by_node[input];
    const auto& expressions_of_input = input->output_expressions();

    for (const auto& required_expression : required_expressions) {
      // Add the columns needed here (and above) if they come from the input node. Reasons why this might NOT be the
      // case are: (1) The expression is calculated in this node (and is thus not available in the input node), or
      // (2) we have two input nodes (i.e., a join) and the expressions comes from the other side.
      if (find_expression_idx(*required_expression, expressions_of_input)) {
        required_expressions_for_input.emplace(required_expression);
      }
    }

    recursively_gather_required_expressions(input, required_expressions_by_node, outputs_visited_by_node);
  }
}

void try_join_to_semi_rewrite(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) {
  // Sometimes, joins are not actually used to combine tables but only to check the existence of a tuple in a second
  // table. Example: SELECT c_name FROM customer, nation WHERE c_nationkey = n_nationkey AND n_name = 'GERMANY'
  // If the join is on a unique/primary key column, we can rewrite these joins into semi joins. If, however, the
  // uniqueness is not guaranteed, we cannot perform the rewrite as non-unique joins could possibly emit a matching
  // line more than once.

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
  if (join_node->join_mode != JoinMode::Inner) return;

  // Check whether the left/right inputs are actually needed by following operators
  auto left_input_is_used = false;
  auto right_input_is_used = false;
  for (const auto& output : node->outputs()) {
    for (const auto& required_expression : required_expressions_by_node.at(output)) {
      if (expression_evaluable_on_lqp(required_expression, *node->left_input())) left_input_is_used = true;
      if (expression_evaluable_on_lqp(required_expression, *node->right_input())) right_input_is_used = true;
    }
  }
  DebugAssert(left_input_is_used || right_input_is_used, "Did not expect a useless join");

  // Early out, if we need output expressions from both input tables.
  if (left_input_is_used && right_input_is_used) return;
  std::cout << "One input is used" << std::endl;

  /**
   * We can only rewrite an inner join to a semi join when it has a join cardinality of 1:1 or n:1, which we check as
   * follows:
   * (1) From all predicates of type Equals, we collect the operand expressions by input node.
   * (2) We determine the input node that should be used for filtering.
   * (3) We check the input node from (2) for a matching single- or multi-expression unique constraint.
   *     a) Found match -> Rewrite to semi join
   *     b) No match    -> Do no rewrite to semi join because we might end up with duplicated input records.
   */
  const auto& join_predicates = join_node->join_predicates();
  auto equals_predicate_expressions_left = ExpressionUnorderedSet{};
  auto equals_predicate_expressions_right = ExpressionUnorderedSet{};
  for (const auto& join_predicate : join_predicates) {
    const auto& predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);
    // Skip predicates that are not of type Equals (because we need n:1 or 1:1 join cardinality)
    if (predicate->predicate_condition != PredicateCondition::Equals) continue;

    // Collect operand expressions table-wise
    for (const auto& operand_expression : {predicate->left_operand(), predicate->right_operand()}) {
      if (join_node->left_input()->has_output_expressions({operand_expression})) {
        equals_predicate_expressions_left.insert(operand_expression);
      } else if (join_node->right_input()->has_output_expressions({operand_expression})) {
        equals_predicate_expressions_right.insert(operand_expression);
      }
    }
  }
  // Early out, if we did not see any Equals-predicates.
  if (equals_predicate_expressions_left.empty() || equals_predicate_expressions_right.empty()) return;

  // Determine, which node to use for Semi-Join-filtering and check for the required uniqueness guarantees
  if (!left_input_is_used &&
      join_node->left_input()->has_matching_unique_constraint(equals_predicate_expressions_left)) {
    join_node->join_mode = JoinMode::Semi;
    const auto temp = join_node->left_input();
    join_node->set_left_input(join_node->right_input());
    join_node->set_right_input(temp);
  }
  if (!right_input_is_used &&
      join_node->right_input()->has_matching_unique_constraint(equals_predicate_expressions_right)) {
    join_node->join_mode = JoinMode::Semi;
  }
}

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

void prune_projection_node(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) {
  // Iterate over the ProjectionNode's expressions and add them to the required expressions if at least one output node
  // requires them
  auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node);

  auto new_node_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  new_node_expressions.reserve(projection_node->node_expressions.size());

  for (const auto& expression : projection_node->node_expressions) {
    for (const auto& output : node->outputs()) {
      const auto& required_expressions = required_expressions_by_node.at(output);
      if (std::find_if(required_expressions.begin(), required_expressions.end(), [&expression](const auto& other) {
            return *expression == *other;
          }) != required_expressions.end()) {
        new_node_expressions.emplace_back(expression);
        break;
      }
    }
  }

  projection_node->node_expressions = new_node_expressions;
}

}  // namespace

namespace opossum {

std::string ColumnPruningRule::name() const {
  static const auto name = std::string{"ColumnPruningRule"};
  return name;
}

void ColumnPruningRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // For each node, required_expressions_by_node will hold the expressions either needed by this node or by one of its
  // successors (i.e., nodes to which this node is an input). After collecting this information, we walk through all
  // identified nodes and perform the pruning.
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet> required_expressions_by_node;

  // Add top-level columns that need to be included as they are the actual output
  const auto output_expressions = lqp_root->output_expressions();
  required_expressions_by_node[lqp_root].insert(output_expressions.cbegin(), output_expressions.cend());

  // Recursively walk through the LQP. We cannot use visit_lqp as we explicitly need to take each path through the LQP.
  // The right side of a diamond might require additional columns - if we only visited each node once, we might miss
  // those. However, we track how many of a node's outputs we have already visited and recurse only once we have seen
  // all of them. That way, the performance should be similar to that of visit_lqp.
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> outputs_visited_by_node;
  recursively_gather_required_expressions(lqp_root, required_expressions_by_node, outputs_visited_by_node);

  // Now, go through the LQP and perform all prunings. This time, it is sufficient to look at each node once.
  for (const auto& [node, required_expressions] : required_expressions_by_node) {
    if (outputs_visited_by_node.at(node) != node->output_count()) {
      std::cout << node->description() << std::endl;
      std::cout << node->output_count() << std::endl;
      std::cout << outputs_visited_by_node.at(node) << std::endl;
    }
    DebugAssert(outputs_visited_by_node.at(node) == node->output_count(),
                "Not all outputs have been visited - is the input LQP corrupt?");
    switch (node->type) {
      case LQPNodeType::Mock:
      case LQPNodeType::StoredTable: {
        // Prune all unused columns from a StoredTableNode
        auto pruned_column_ids = std::vector<ColumnID>{};
        const auto& node_output_expressions = node->output_expressions();
        for (const auto& expression : node_output_expressions) {
          if (required_expressions.find(expression) != required_expressions.end()) {
            continue;
          }

          const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
          pruned_column_ids.emplace_back(column_expression->original_column_id);
        }

        if (pruned_column_ids.size() == node_output_expressions.size()) {
          // All columns were marked to be pruned. However, while `SELECT 1 FROM table` does not need any particular
          // column, it needs at least one column so that it knows how many 1s to produce. Thus, we remove a random
          // column from the pruning list. It does not matter which column it is.
          pruned_column_ids.resize(pruned_column_ids.size() - 1);
        }

        if (auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node)) {
          DebugAssert(stored_table_node->pruned_column_ids().empty(), "Node pruned twice");
          stored_table_node->set_pruned_column_ids(pruned_column_ids);
        } else if (auto mock_node = std::dynamic_pointer_cast<MockNode>(node)) {
          DebugAssert(mock_node->pruned_column_ids().empty(), "Node pruned twice");
          mock_node->set_pruned_column_ids(pruned_column_ids);
        }
      } break;

      case LQPNodeType::Join: {
        // Prevent the application of the join-to-semi-join strategy to make our join-to-local-predicate-rewrite strategy applicable.
        try_join_to_semi_rewrite(node, required_expressions_by_node);
        try_join_to_local_predicate_rewrite(node, required_expressions_by_node);
      } break;

      case LQPNodeType::Projection: {
        prune_projection_node(node, required_expressions_by_node);
      } break;

      default:
        break;  // Node cannot be pruned
    }
  }
}

}  // namespace opossum
