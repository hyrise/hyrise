#include "pqp_analyzer.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/pqp_utils.hpp"
#include "utils/timer.hpp"

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
    const auto pqp_root = entry.value;
    visit_pqp(pqp_root, [&](const auto& op) {
      const auto lqp_node = op->lqp_node;
      if (!lqp_node) {
        return PQPVisitation::VisitInputs;
      }
      auto prio = static_cast<size_t>(op->performance_data->walltime.count());
      switch (op->type()) {
        case OperatorType::JoinHash:
        case OperatorType::JoinNestedLoop:
        case OperatorType::JoinSortMerge: {
          // To DO: Join to SemiJoin rewrite; Join to Scan OD / UCC
          // join to semi rewrite: UCC on one/both join columns
          // join to scan:  - scan on one input
          //                - between predicate == OD scan predicate --> join predicate
          //                - equals predicate == UCC scan prediacte
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
          // std::cout << join_node->description() << std::endl;
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
              const auto join_column = static_pointer_cast<LQPColumnExpression>(expression);
              // std::cout << "join column id" << std::endl;
              const auto join_column_id = _resolve_column_expression(expression);
              if (join_column_id == INVALID_TABLE_COLUMN_ID) {
                continue;
              }
              if (join_node->join_mode == JoinMode::Inner) {
                auto candidate = DependencyCandidate{std::vector<TableColumnID>{join_column_id},
                                                     std::vector<TableColumnID>{}, DependencyType::Unique, prio};
                _add_if_new(candidate);
              }
              bool abort = false;
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
                          my_candidates.emplace_back(std::vector<TableColumnID>{scan_column_id},
                                                     std::vector<TableColumnID>{}, DependencyType::Unique, prio);
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
                          my_candidates.emplace_back(std::vector<TableColumnID>{scan_column_id},
                                                     std::vector<TableColumnID>{join_column_id}, DependencyType::Order,
                                                     prio);
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
          std::vector<TableColumnID> determinants;
          for (auto expression_idx = size_t{0}; expression_idx < num_group_by_columns; ++expression_idx) {
            if (node_expressions[expression_idx]->type != ExpressionType::LQPColumn) {
              continue;
            }
            auto table_column_id = _resolve_column_expression(node_expressions[expression_idx]);
            if (table_column_id != INVALID_TABLE_COLUMN_ID) {
              determinants.emplace_back(table_column_id);
            }
          }
          if (determinants.empty()) {
            return PQPVisitation::VisitInputs;
          }
          auto candidate =
              DependencyCandidate{determinants, std::vector<TableColumnID>{}, DependencyType::Functional, prio};
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

std::vector<TableColumnID> PQPAnalyzer::_find_od_candidate(
    const std::shared_ptr<const AbstractOperator>& op, const std::shared_ptr<LQPColumnExpression>& dependent) const {
  std::vector<TableColumnID> candidates;
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
