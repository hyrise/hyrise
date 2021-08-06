#include "pqp_analyzer.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/pqp_utils.hpp"
#include "utils/timer.hpp"

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
    const std::shared_ptr<const AbstractLQPNode>& node, const ExpressionUnorderedSet& expressions_required_by_consumers) {
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
      const auto& aggregate_node = static_cast<const AggregateNode&>(*node);
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
      const auto& join_node = static_cast<const JoinNode&>(*node);
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
    const std::shared_ptr<const AbstractLQPNode>& node,
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node,
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t>& outputs_visited_by_node) {
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
}

namespace opossum {

// void PQPAnalyzer::set_queue(const DependencyCandidateQueue& queue) { _queue = queue; };

PQPAnalyzer::PQPAnalyzer(const std::shared_ptr<DependencyCandidateQueue>& queue) : _queue(queue) {}

void PQPAnalyzer::run() {
  const auto& pqp_cache = Hyrise::get().default_pqp_cache;
  if (!pqp_cache) {
    std::cout << "NO PQPCache. Stopping" << std::endl;
    return;
  }
  const auto cache_snapshot = pqp_cache->snapshot();

  if (cache_snapshot.empty()) {
    std::cout << "PQPCache empty. Stopping" << std::endl;
    return;
  }

  std::cout << "Run PQPAnalyzer" << std::endl;
  Timer timer;

  for (const auto& [_, entry] : cache_snapshot) {
    // std::cout << std::endl << query << std::endl;
    const auto pqp_root = entry.value;
    const auto& lqp_root = pqp_root->lqp_node;
    if (!lqp_root) {
      std::cout << " No LQP root found" << std::endl;
      continue;
    }
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet> required_expressions_by_node;

    // Add top-level columns that need to be included as they are the actual output
    const auto output_expressions = lqp_root->output_expressions();
    required_expressions_by_node[lqp_root].insert(output_expressions.cbegin(), output_expressions.cend());

    // Recursively walk through the LQP. We cannot use visit_lqp as we explicitly need to take each path through the LQP.
    // The right side of a diamond might require additional columns - if we only visited each node once, we might miss
    // those. However, we track how many of a node's outputs we have already visited and recurse only once we have seen
    // all of them. That way, the performance should be similar to that of visit_lqp.
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t> outputs_visited_by_node;
    recursively_gather_required_expressions(lqp_root, required_expressions_by_node, outputs_visited_by_node);

    visit_pqp(pqp_root, [&](const auto& op) {
      const auto& lqp_node = op->lqp_node;
      if (!lqp_node) {
        return PQPVisitation::VisitInputs;
      }
      auto prio = static_cast<size_t>(op->performance_data->walltime.count());
      switch (op->type()) {
        case OperatorType::JoinHash:
        case OperatorType::JoinNestedLoop:
        case OperatorType::JoinSortMerge: {
          const auto join_node = static_pointer_cast<const JoinNode>(lqp_node);
          if (join_node->join_mode != JoinMode::Semi && join_node->join_mode != JoinMode::Inner) {
            return PQPVisitation::VisitInputs;
          }

          const auto& predicates = join_node->join_predicates();
          if (predicates.size() != 1) {
            return PQPVisitation::VisitInputs;
          }
          const auto& predicate = std::static_pointer_cast<AbstractPredicateExpression>(predicates[0]);
          std::vector<std::shared_ptr<AbstractLQPNode>> inputs;
          std::cout << join_node->description() << " " << lqp_node->comment << std::endl;
          inputs.emplace_back(join_node->right_input());
          if (join_node->join_mode == JoinMode::Inner) {
            inputs.emplace_back(join_node->left_input());
          }

          const auto& predicate_arguments = predicate->arguments;
          for (const auto& input : inputs) {
            for (const auto& expression : predicate_arguments) {
              if (!expression_evaluable_on_lqp(expression, *input) || expression->type != ExpressionType::LQPColumn) {
                continue;
              }
              // std::cout << " " << input->description() << "\t" << expression->description() << std::endl;
              const auto join_column = static_pointer_cast<LQPColumnExpression>(expression);
              // std::cout << "    " << expression->description() << std::endl;
              const auto join_column_id = _resolve_column_expression(expression);
              if (join_column_id == INVALID_TABLE_COLUMN_ID) {
                continue;
              }
              bool abort = false;
              for (const auto& output : join_node->outputs()) {
                if (abort) break;
                if (required_expressions_by_node.find(output) == required_expressions_by_node.end()) {
                  std::cout << " Could not find ouput node " << output->description() << std::endl;
                  abort = true;
                  break;
                }
                for (const auto& required_expression : required_expressions_by_node.at(output)) {
                  // abort if any column other than join column required
                  if (expression_evaluable_on_lqp(required_expression, *input) && *required_expression != *expression) {
                    // std::cout << " abort due " << required_expression->description() << std::endl;
                    abort = true;
                    break;
                  }
                }
              }
              /*for (const auto& join_output : join_outputs) {
                if (expression_evaluable_on_lqp(join_output, *input) && *join_output != *expression) {
                  std::cout << "        abort due " << join_output->description() << std::endl;
                  abort = true;
                  break;
                }
              }*/
              if (abort) continue;

              if (join_node->join_mode == JoinMode::Inner) {
                auto candidate = DependencyCandidate{TableColumnIDs{join_column_id},
                                                     {}, DependencyType::Unique, prio};
                _add_if_new(candidate);
              }

              std::vector<DependencyCandidate> my_candidates;
              visit_lqp(input, [&](const auto& node) {
                switch (node->type) {
                  case LQPNodeType::Validate:
                    return LQPVisitation::VisitInputs;
                  case LQPNodeType::StoredTable:
                  case LQPNodeType::StaticTable:
                    return LQPVisitation::DoNotVisitInputs;
                  case LQPNodeType::Predicate: {
                    const auto predicate_node = static_pointer_cast<PredicateNode>(node);
                    const auto scan_predicate = predicate_node->predicate();
                    const auto predicate_expression = static_pointer_cast<AbstractPredicateExpression>(scan_predicate);
                    if (predicate_expression->predicate_condition == PredicateCondition::Equals) {
                      const auto scan_inputs = predicate_expression->arguments;
                      for (const auto& scan_input : scan_inputs) {
                        if (scan_input->type == ExpressionType::LQPColumn) {
                          // std::cout << "equals scan column id" << std::endl;
                          const auto scan_column_id = _resolve_column_expression(scan_input);
                          if (scan_column_id == INVALID_TABLE_COLUMN_ID) {
                            continue;
                          }
                          my_candidates.emplace_back(TableColumnIDs{scan_column_id},
                                                     TableColumnIDs{}, DependencyType::Unique, prio);
                          // std::cout << "        added " << scan_input->description()  << " UCC" << std::endl;
                        }
                      }
                    }
                    if (is_between_predicate_condition(predicate_expression->predicate_condition)) {
                      const auto scan_inputs = predicate_expression->arguments;
                      for (const auto& scan_input : scan_inputs) {
                        if (scan_input->type == ExpressionType::LQPColumn) {
                          // std::cout << "between scan column id" << std::endl;
                          const auto scan_column_id = _resolve_column_expression(scan_input);
                          if (scan_column_id == INVALID_TABLE_COLUMN_ID) {
                            continue;
                          }
                          my_candidates.emplace_back(TableColumnIDs{scan_column_id},
                                                     TableColumnIDs{join_column_id}, DependencyType::Order,
                                                     prio);
                          // std::cout << "        added " << scan_input->description() << " OD" << std::endl;
                        }
                      }
                    }
                  }
                    return LQPVisitation::VisitInputs;
                  default: {
                    abort = true;
                  }
                    return LQPVisitation::DoNotVisitInputs;
                }
              });
              if (!abort) {
                for (auto& candidate : my_candidates) {
                  _add_if_new(candidate);
                }
              } /* else {
                std::cout << "aborted" << std::endl;
              }*/
            }
          }
        } break;
        case OperatorType::Aggregate: {
          const auto aggregate_node = static_pointer_cast<const AggregateNode>(lqp_node);
          const auto num_group_by_columns = aggregate_node->aggregate_expressions_begin_idx;
          if (num_group_by_columns < 2) {
            return PQPVisitation::VisitInputs;
          }
          const auto& node_expressions = aggregate_node->node_expressions;
          // split columns by table to ease validation later on
          TableColumnIDs columns;
          for (auto expression_idx = size_t{0}; expression_idx < num_group_by_columns; ++expression_idx) {
            if (node_expressions[expression_idx]->type != ExpressionType::LQPColumn) {
              continue;
            }
            auto table_column_id = _resolve_column_expression(node_expressions[expression_idx]);
            if (table_column_id != INVALID_TABLE_COLUMN_ID) {
              columns.emplace_back(table_column_id);
            }
          }
          if (columns.size() < 2) {
            return PQPVisitation::VisitInputs;
          }
          auto candidate = DependencyCandidate{columns, {}, DependencyType::Functional, prio};
          _add_if_new(candidate);
        } break;
        default:
          break;
      }

      return PQPVisitation::VisitInputs;
    });
  }
  if (_queue) {
    for (auto& candidate : _known_candidates) {
      _queue->emplace(candidate);
    }
  }

  if (Hyrise::get().storage_manager.has_table("nation")) {
    const auto nation = Hyrise::get().storage_manager.get_table("nation");
    const auto n_nationkey = TableColumnID{"nation", nation->column_id_by_name("n_nationkey")};
    const auto n_name = TableColumnID{"nation", nation->column_id_by_name("n_name")};

    const auto customer = Hyrise::get().storage_manager.get_table("customer");
    const auto c_custkey = TableColumnID{"customer", customer->column_id_by_name("c_custkey")};

    const auto orders = Hyrise::get().storage_manager.get_table("orders");
    const auto o_orderkey = TableColumnID{"orders", orders->column_id_by_name("o_orderkey")};

    _queue->emplace(TableColumnIDs{c_custkey}, TableColumnIDs{n_nationkey}, DependencyType::Inclusion, 1);
    _queue->emplace(TableColumnIDs{n_nationkey}, TableColumnIDs{n_nationkey}, DependencyType::Inclusion, 1);
    _queue->emplace(TableColumnIDs{o_orderkey}, TableColumnIDs{c_custkey}, DependencyType::Inclusion, 1);
    _queue->emplace(TableColumnIDs{n_nationkey}, TableColumnIDs{c_custkey}, DependencyType::Inclusion, 1);
    _queue->emplace(TableColumnIDs{n_nationkey}, TableColumnIDs{n_name}, DependencyType::Inclusion, 1);
  }

  std::cout << "PQPAnalyzer finished in " << timer.lap_formatted() << std::endl;
}

TableColumnID PQPAnalyzer::_resolve_column_expression(
    const std::shared_ptr<AbstractExpression>& column_expression) const {
  Assert(column_expression->type == ExpressionType::LQPColumn, "Expected LQPColumnExpression");
  const auto lqp_column_expression = static_pointer_cast<LQPColumnExpression>(column_expression);
  const auto orig_node = lqp_column_expression->original_node.lock();
  if (orig_node->type != LQPNodeType::StoredTable) {
    return INVALID_TABLE_COLUMN_ID;
  }
  const auto original_column_id = lqp_column_expression->original_column_id;
  if (original_column_id == INVALID_COLUMN_ID) {
    return INVALID_TABLE_COLUMN_ID;
  }
  const auto stored_table_node = static_pointer_cast<const StoredTableNode>(orig_node);
  const auto table_name = stored_table_node->table_name;
  return TableColumnID{table_name, original_column_id};
}

TableColumnIDs PQPAnalyzer::_find_od_candidate(
    const std::shared_ptr<const AbstractOperator>& op, const std::shared_ptr<LQPColumnExpression>& dependent) const {
  TableColumnIDs candidates;
  visit_pqp(op, [&](const auto& current_op) {
    switch (current_op->type()) {
      case OperatorType::Validate:
        return PQPVisitation::VisitInputs;
      default:
        return PQPVisitation::DoNotVisitInputs;
    }
  });

  return candidates;
}

void PQPAnalyzer::_add_if_new(DependencyCandidate& candidate) {
  for (auto& known_candidate : _known_candidates) {
    if (known_candidate.type != candidate.type) {
      continue;
    }
    if (candidate.dependents == known_candidate.dependents && candidate.determinants == known_candidate.determinants) {
      if (known_candidate.priority < candidate.priority) {
        known_candidate.priority = candidate.priority;
      }
      return;
    }
  }
  _known_candidates.emplace_back(candidate);
}

}  // namespace opossum
