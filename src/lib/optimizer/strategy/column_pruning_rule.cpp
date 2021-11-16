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
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/segment_iterate.hpp"

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

      for (auto expression_idx = size_t{0}; expression_idx < node_expressions.size(); ++expression_idx) {
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
      for (const auto& expression : node->node_expressions) {
        if (node->type == LQPNodeType::Projection && !expressions_required_by_consumers.contains(expression)) {
          // An expression produced by a ProjectionNode that is not required by anyone upstream is useless. We should
          // not collect the expressions required for calculating that useless expression.
          continue;
        }

        gather_expressions_not_computed_by_expression_evaluator(expression, node->left_input()->output_expressions(),
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
    for (const auto& required_expression : required_expressions) {
      // Add the columns needed here (and above) if they come from the input node. Reasons why this might NOT be the
      // case are: (1) The expression is calculated in this node (and is thus not available in the input node), or
      // (2) we have two input nodes (i.e., a join) and the expressions comes from the other side.
      if (input->find_column_id(*required_expression)) {
        required_expressions_for_input.emplace(required_expression);
      }
    }

    recursively_gather_required_expressions(input, required_expressions_by_node, outputs_visited_by_node);
  }
}

// UCC and OD based rewrites from Joins to Scans
// currently, does rewrite only using one possible predicate
bool try_join_to_scan_rewrite(
    const std::shared_ptr<JoinNode>& join, ExpressionUnorderedSet& equals_predicate_expressions_used_side,
    ExpressionUnorderedSet& equals_predicate_expressions_unused_side, LQPInputSide used_side,
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) {
  const auto used_input = join->input(used_side);
  const auto unused_side = used_side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left;
  const auto unused_input = join->input(unused_side);
  //std::cout << "try join2pred " << unused_input->description() << std::endl;

  // early out if input is required by another node or join column is not unique
  if (unused_input->output_count() > 1) return false;
  const auto join_expression = *(equals_predicate_expressions_unused_side.begin());
  if (!unused_input->has_matching_unique_constraint(equals_predicate_expressions_unused_side)) return false;

  std::vector<std::shared_ptr<PredicateNode>> scans;
  bool abort = false;

  // find candidates for optimization
  visit_lqp(unused_input, [&](auto node) {
    if (abort) {
      return LQPVisitation::DoNotVisitInputs;
    }
    switch (node->type) {
      case LQPNodeType::Validate:
        return LQPVisitation::VisitInputs;
      case LQPNodeType::StoredTable:
      case LQPNodeType::StaticTable:
        return LQPVisitation::DoNotVisitInputs;
      // multiple scans are ok, just find the one with the correct column later
      case LQPNodeType::Predicate: {
        scans.emplace_back(static_pointer_cast<PredicateNode>(node));
        return LQPVisitation::VisitInputs;
      }
      // more complex stucture beforehand, just as (semi-)joins, would work as well
      // but their sub-lqps would hardly be optimized
      default: {
        abort = true;
        return LQPVisitation::DoNotVisitInputs;
      }
    }
  });

  if (abort || scans.empty()) {
    return false;
  }

  const auto execute_subplan = [](auto& sub_plan_root, const auto scan_column_id, const auto value_column_id) {
    sub_plan_root->clear_outputs();

    // prune columns
    visit_lqp(sub_plan_root, [&](const auto& node) {
      if (node->type == LQPNodeType::StoredTable) {
        std::vector<ColumnID> pruned_column_ids;
        const auto stored_table_node = static_pointer_cast<StoredTableNode>(node);
        const auto num_columns = Hyrise::get().storage_manager.get_table(stored_table_node->table_name)->column_count();
        for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
          if (column_id != scan_column_id && column_id != value_column_id) {
            pruned_column_ids.emplace_back(column_id);
          }
        }
        stored_table_node->set_pruned_column_ids(pruned_column_ids);
      }
      return LQPVisitation::VisitInputs;
    });

    // prune chunks
    auto chunk_pruning_rule = ChunkPruningRule{};
    const auto sub_root = LogicalPlanRootNode::make(std::move(sub_plan_root));
    chunk_pruning_rule.apply_to_plan(sub_root);
    auto optimized_sub = sub_root->left_input();
    sub_root->set_left_input(nullptr);

    // execute subplan
    const auto sub_root_pqp = LQPTranslator{}.translate_node(optimized_sub);
    sub_root_pqp->never_clear_output();
    const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);
    sub_root_pqp->set_transaction_context_recursively(transaction_context);
    const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(sub_root_pqp);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
    return sub_root_pqp->get_output();
  };

  const auto replace_node = [&](const auto& original_node, const auto& new_node, const auto& unused_column) {
    original_node->set_left_input(used_input);
    original_node->set_right_input(nullptr);
    lqp_replace_node(original_node, new_node);
    auto& new_node_required_expressions = required_expressions_by_node[new_node];
    for (const auto& required_expression : required_expressions_by_node.at(original_node)) {
      if (required_expression != unused_column) {
        new_node_required_expressions.emplace(required_expression);
      }
    }
  };

  const auto join_column_expression = static_pointer_cast<LQPColumnExpression>(join_expression);
  const auto target_column_id = join_column_expression->original_column_id;

  bool executed = false;
  for (const auto& scan : scans) {
    const auto scan_predicate = scan->predicate();
    const auto predicate_expression = static_pointer_cast<AbstractPredicateExpression>(scan_predicate);

    if (predicate_expression->predicate_condition == PredicateCondition::Equals) {
      visit_expression(scan_predicate, [&](auto expression) {
        if (expression->type == ExpressionType::LQPColumn) {
          Assert(!executed, "Did not expect mutiple scan columns");
          auto ref_expressions = ExpressionUnorderedSet{expression};
          if (scan->has_matching_unique_constraint(ref_expressions)) {
            executed = true;
            const auto filter_column_id = static_pointer_cast<LQPColumnExpression>(expression)->original_column_id;
            const auto tab = execute_subplan(unused_input, filter_column_id, target_column_id);
            const auto value_column_id = target_column_id < filter_column_id ? ColumnID{0} : ColumnID{1};
            const auto target_column_type = tab->column_definitions().at(value_column_id).data_type;
            resolve_data_type(target_column_type, [&](auto type) {
              using ColumnDataType = typename decltype(type)::type;
              const auto num_chunks = tab->chunk_count();
              ColumnDataType val{};
              bool is_init = false;
              for (auto chunk_id = ChunkID{0}; chunk_id < num_chunks; ++chunk_id) {
                const auto chunk = tab->get_chunk(chunk_id);
                if (!chunk) {
                  continue;
                }
                const auto segment = chunk->get_segment(value_column_id);
                segment_iterate<ColumnDataType>(*segment, [&](const auto& pos) {
                  Assert(!is_init, "did not expect multiple values");
                  val = pos.value();
                  is_init = true;
                });
              }
              const auto scan_column_expression = *equals_predicate_expressions_used_side.begin();
              const auto predicate_node = PredicateNode::make(equals_(scan_column_expression, val));
              replace_node(join, predicate_node, join_column_expression);
              //std::cout << "rewrote Join2Pred (i)" << std::endl;
            });
          }
        }
        return ExpressionVisitation::VisitArguments;
      });
    }
    if (is_between_predicate_condition(predicate_expression->predicate_condition)) {
      const auto order_dependencies = unused_input->order_dependencies();
      if (order_dependencies.empty()) {
        return false;
      }
      visit_expression(scan_predicate, [&](auto expression) {
        if (expression->type == ExpressionType::LQPColumn) {
          Assert(!executed, "Did not expect mutiple scan columns");
          for (const auto& od : order_dependencies) {
            // skip larger ODs
            if (od.determinants.size() != 1 || od.dependents.size() != 1) {
              continue;
            }
            if (*od.determinants[0] == *expression && *od.dependents[0] == *join_expression) {
              executed = true;
              const auto filter_column_id = static_pointer_cast<LQPColumnExpression>(expression)->original_column_id;
              const auto tab = execute_subplan(unused_input, filter_column_id, target_column_id);
              const auto value_column_id = target_column_id < filter_column_id ? ColumnID{0} : ColumnID{1};
              const auto target_column_type = tab->column_definitions().at(value_column_id).data_type;
              resolve_data_type(target_column_type, [&](auto type) {
                using ColumnDataType = typename decltype(type)::type;
                const auto num_chunks = tab->chunk_count();
                ColumnDataType min_val{};
                ColumnDataType max_val{};
                bool is_init = false;
                for (auto chunk_id = ChunkID{0}; chunk_id < num_chunks; ++chunk_id) {
                  const auto chunk = tab->get_chunk(chunk_id);
                  if (!chunk) {
                    continue;
                  }
                  const auto segment = chunk->get_segment(value_column_id);
                  segment_iterate<ColumnDataType>(*segment, [&](const auto& pos) {
                    const auto current_value = pos.value();
                    if (!is_init) {
                      min_val = current_value;
                      max_val = current_value;
                      is_init = true;
                    } else {
                      min_val = std::min(min_val, current_value);
                      max_val = std::max(max_val, current_value);
                    }
                  });
                }
                const auto scan_column_expression = *equals_predicate_expressions_used_side.begin();
                const auto predicate_node =
                    PredicateNode::make(between_inclusive_(scan_column_expression, min_val, max_val));
                replace_node(join, predicate_node, join_column_expression);
                //std::cout << "rewrote Join2Pred (ii)" << std::endl;
              });
            }
          }
        }
        return ExpressionVisitation::VisitArguments;
      });
    }
  }
  return executed;
}

// IND and UCC-based elimination of Joins
void try_eliminate_join(const std::shared_ptr<JoinNode>& join,
                        ExpressionUnorderedSet& equals_predicate_expressions_used_side,
                        ExpressionUnorderedSet& equals_predicate_expressions_unused_side, LQPInputSide unused_side) {
  //std::cout << 438 << std::endl;
  // std::cout << "try rewrite " << join->description() << " with IND" << std::endl;
  auto unused_input = join->input(unused_side);
  //std::cout << " try_eliminate_join " << unused_input->description() << std::endl;

  if (unused_input->output_count() > 1) return;
  // std::cout << 449 << std::endl;
  if (!unused_input->has_matching_unique_constraint(equals_predicate_expressions_unused_side)) return;
  // std::cout << 451 << std::endl;
  bool abort = false;
  visit_lqp(unused_input, [&unused_input, &abort](auto& node) {
    switch (node->type) {
      case LQPNodeType::Validate:
        return LQPVisitation::VisitInputs;
      case LQPNodeType::StoredTable: {
        unused_input = node;
      }
        return LQPVisitation::DoNotVisitInputs;
      default: {
        // std::cout << " abort " << node->description() << std::endl;
        abort = true;
      }
        return LQPVisitation::DoNotVisitInputs;
    }
  });

  if (abort || unused_input->type != LQPNodeType::StoredTable) return;
  //std::cout << 470 << std::endl;
  // std::cout << " 471 " << unused_input->description() << std::endl;
  const auto& stored_table_node = static_cast<StoredTableNode&>(*unused_input);
  // std::cout << 473 << std::endl;
  const auto used_side = unused_side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left;
  const auto used_input = join->input(used_side);
  const auto used_side_join_expression = *(equals_predicate_expressions_used_side.begin());
  const auto used_side_join_column_expression = static_pointer_cast<LQPColumnExpression>(used_side_join_expression);
  const auto unused_side_join_expression = *(equals_predicate_expressions_unused_side.begin());
  const auto unused_side_join_column_expression = static_pointer_cast<LQPColumnExpression>(unused_side_join_expression);
  const auto unused_side_table_name = stored_table_node.table_name;
  const auto unused_side_column_id = unused_side_join_column_expression->original_column_id;
  // std::cout << 482 << std::endl;
  // std::cout << "    used: " << resolve_column_expression(used_side_join_expression).table_name << "    unused: " << unused_side_table_name << std::endl;
  for (const auto& ind : used_input->inclusion_dependencies()) {
    // std::cout << "    try: " << ind << std::endl;
    if (ind.determinants.size() != 1 || ind.dependents.size() != 1) continue;
    const auto& determinant = ind.determinants.at(0);
    const auto& dependent = ind.dependents.at(0);
    if (*dependent != *used_side_join_column_expression) continue;
    // std::cout << 490 << std::endl;
    if (determinant.table_name == unused_side_table_name && determinant.column_id == unused_side_column_id) {
      // std::cout << "rewrite " << join->description() << "    with " << ind << std::endl;
      join->set_left_input(used_input);
      join->set_right_input(nullptr);
      lqp_remove_node(join);
      // std::cout << "rewrote JoinElimination    " << join->description() << std::endl;
      //std::cout << 467 << std::endl;
      return;
    }
  }
}

void try_join_to_semi_rewrite(
    const std::shared_ptr<AbstractLQPNode>& node,
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) {
  // Sometimes, joins are not actually used to combine tables but only to check the existence of a tuple in a second
  // table. Example: SELECT c_name FROM customer, nation WHERE c_nationkey = n_nationkey AND n_name = 'GERMANY'
  // If the join is on a unique/primary key column, we can rewrite these joins into semi joins. If, however, the
  // uniqueness is not guaranteed, we cannot perform the rewrite as non-unique joins could possibly emit a matching
  // line more than once.

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
  if (join_node->join_mode != JoinMode::Inner) return;
  //std::cout << join_node->description() << std::endl;

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
  // TO DO: can rewrite later expression on other table
  if (left_input_is_used && right_input_is_used) return;

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

  bool flipped_inputs = false;
  DebugAssert(Hyrise::get().dependency_usage_config, "No DependencyUsageConfig set");
  if (Hyrise::get().dependency_usage_config->enable_join_to_semi) {
    // Determine, which node to use for Semi-Join-filtering and check for the required uniqueness guarantees
    if (!left_input_is_used &&
        join_node->left_input()->has_matching_unique_constraint(equals_predicate_expressions_left)) {
      join_node->join_mode = JoinMode::Semi;
      const auto temp = join_node->left_input();
      join_node->set_left_input(join_node->right_input());
      join_node->set_right_input(temp);
      flipped_inputs = true;
      //std::cout << "rewrote Join2Semi (i)" << std::endl;
      //std::cout << "rewrite " << join_node->description() << " with ";
      //for (const auto& e : equals_predicate_expressions_left) {
      //  std::cout << " " << e->description();
      //}
      //std::cout << std::endl;
    }
    if (!right_input_is_used &&
        join_node->right_input()->has_matching_unique_constraint(equals_predicate_expressions_right)) {
      join_node->join_mode = JoinMode::Semi;
      //std::cout << "rewrote Join2Semi (ii)" << std::endl;
      //std::cout << "rewrite " << join_node->description() << " with ";
      //for (const auto& e : equals_predicate_expressions_right) {
      //  std::cout << " " << e->description();
      //}
      //std::cout << std::endl;
    }
  }
  if (equals_predicate_expressions_left.size() != 1 || equals_predicate_expressions_right.size() != 1) {
    return;
  }
  if (Hyrise::get().dependency_usage_config->enable_join_to_predicate) {
    if (!left_input_is_used) {
      const auto used_input_side = flipped_inputs ? LQPInputSide::Left : LQPInputSide::Right;
      //std::cout << 556 << std::endl;
      // If join is rewritten to scan, there has been a predicate before, and we cannot eliminate it with INDs.
      // Addidtionally, the join is not in the LQP anymore.
      if (try_join_to_scan_rewrite(join_node, equals_predicate_expressions_right, equals_predicate_expressions_left,
                                   used_input_side, required_expressions_by_node)) {
        //std::cout << "rewrote " << join_node->description() << " to predicate (i)" << std::endl;
        return;
      }
      //std::cout << 559 << std::endl;
    }
    if (!right_input_is_used) {
      //std::cout << 562 << std::endl;
      if (try_join_to_scan_rewrite(join_node, equals_predicate_expressions_left, equals_predicate_expressions_right,
                                   LQPInputSide::Left, required_expressions_by_node)) {
        //std::cout << "rewrote " << join_node->description() << " to predicate (ii)" << std::endl;
        return;
      }
      //std::cout << 565 << std::endl;
    }
  }
  if (Hyrise::get().dependency_usage_config->enable_join_elimination) {
    if (!left_input_is_used) {
      const auto unused_input_side = flipped_inputs ? LQPInputSide::Right : LQPInputSide::Left;
      //std::cout << 571 << std::endl;
      try_eliminate_join(join_node, equals_predicate_expressions_right, equals_predicate_expressions_left,
                         unused_input_side);
      //std::cout << 574 << std::endl;
    }
    if (!right_input_is_used) {
      //std::cout << 577 << std::endl;
      try_eliminate_join(join_node, equals_predicate_expressions_left, equals_predicate_expressions_right,
                         LQPInputSide::Right);
      //std::cout << 580 << std::endl;
    }
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

  // We need to add rewritten scans to the required expressions map to enable rewrites of possible input joins.
  // Thus, we cannot iterate over the map while manipulating it.
  std::vector<std::shared_ptr<AbstractLQPNode>> nodes_to_visit;
  nodes_to_visit.reserve(required_expressions_by_node.size());
  for (const auto& [node, _] : required_expressions_by_node) {
    nodes_to_visit.emplace_back(node);
  }

  // Now, go through the LQP and perform all prunings. This time, it is sufficient to look at each node once.
  for (const auto& node : nodes_to_visit) {
    DebugAssert(outputs_visited_by_node.at(node) == node->output_count(),
                "Not all outputs have been visited - is the input LQP corrupt?");
    const auto& required_expressions = required_expressions_by_node.at(node);
    switch (node->type) {
      case LQPNodeType::Mock:
      case LQPNodeType::StoredTable: {
        // Prune all unused columns from a StoredTableNode
        auto pruned_column_ids = std::vector<ColumnID>{};
        for (const auto& expression : node->output_expressions()) {
          if (required_expressions.find(expression) != required_expressions.end()) {
            continue;
          }

          const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
          pruned_column_ids.emplace_back(column_expression->original_column_id);
        }

        if (pruned_column_ids.size() == node->output_expressions().size()) {
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
        try_join_to_semi_rewrite(node, required_expressions_by_node);
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
