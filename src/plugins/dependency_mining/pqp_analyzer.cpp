#include "pqp_analyzer.hpp"

#include "expression/lqp_column_expression.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/pqp_utils.hpp"

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

  std::vector<DependencyCandidate> current_candidates;

  for (const auto& [_, entry] : cache_snapshot) {
    const auto pqp_root = entry.value;
    visit_pqp(pqp_root, [&](const auto& op) {
      const auto lqp_node = op->lqp_node;
      if (!lqp_node) {
        return PQPVisitation::VisitInputs;
      }

      switch (op->type()) {
        case OperatorType::JoinHash:
        case OperatorType::JoinNestedLoop:
        case OperatorType::JoinSortMerge: {
          // To DO: Join to SemiJoin rewrite; Join to Scan OD
          const auto join_node = static_pointer_cast<const JoinNode>(lqp_node);
          if (join_node->join_mode != JoinMode::Semi && join_node->join_mode != JoinMode::Inner) {
            return PQPVisitation::VisitInputs;
          }
          std::cout << join_node->description() << std::endl;
        } break;
        case OperatorType::Aggregate: {
          const auto aggregate_node = static_pointer_cast<const AggregateNode>(lqp_node);
          const auto num_group_by_columns = aggregate_node->aggregate_expressions_begin_idx;
          if (num_group_by_columns < 2) {
            return PQPVisitation::VisitInputs;
          }
          const auto& node_expressions = aggregate_node->node_expressions;
          std::vector<TableColumnID> determinants;
          std::vector<TableColumnID> dependents;
          for (auto expression_idx = size_t{0}; expression_idx < num_group_by_columns; ++expression_idx) {
            auto table_column_id = _resolve_column_expression(node_expressions[expression_idx]);
            if (table_column_id != INVALID_TABLE_COLUMN_ID) {
              determinants.emplace_back(table_column_id);
            }
          }
          for (auto expression_idx = size_t{num_group_by_columns}; expression_idx < node_expressions.size(); ++expression_idx) {
            visit_expression(node_expressions[expression_idx], [&](auto& expression){
              if (expression->type == ExpressionType::LQPColumn) {
                auto table_column_id = _resolve_column_expression(expression);
                if (table_column_id != INVALID_TABLE_COLUMN_ID) {
                  dependents.emplace_back(table_column_id);
                }
              }
              return ExpressionVisitation::VisitArguments;
            });
          }
          if (determinants.empty() || dependents.empty()) {
            return PQPVisitation::VisitInputs;
          }
          std::cout << "Dependent Group-by Reduction candidates" << std::endl;
          for (const auto& [table_name, column_id] : determinants) {
            const auto orig_table = Hyrise::get().storage_manager.get_table(table_name);
            const auto column_name = orig_table->column_name(column_id);
            std::cout << "\t" << table_name << "." << column_name;
          }
          std::cout << "\t-->";
          for (const auto& [table_name, column_id] : dependents) {
            const auto orig_table = Hyrise::get().storage_manager.get_table(table_name);
            const auto column_name = orig_table->column_name(column_id);
            std::cout << "\t" << table_name << "." << column_name;
          }
          std::cout << std::endl;
          auto prio = static_cast<size_t>(op->performance_data->walltime.count());
          current_candidates.emplace_back(determinants, dependents, DependencyType::Functional, prio);
        } break;
        default:
          break;
      }

      return PQPVisitation::VisitInputs;
    });
  }
  if (_queue) {
    _queue->emplace(std::vector<TableColumnID>{}, std::vector<TableColumnID>{}, DependencyType::Order, 1);
    _queue->emplace(std::vector<TableColumnID>{}, std::vector<TableColumnID>{}, DependencyType::Inclusion, 2);
    for (auto& candidate : current_candidates) {
      _queue->emplace(candidate);
    }
  }
}

TableColumnID PQPAnalyzer::_resolve_column_expression(const std::shared_ptr<AbstractExpression>& column_expression) const {
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


std::vector<TableColumnID> PQPAnalyzer::_find_od_candidate(const std::shared_ptr<const AbstractOperator>& op, const std::shared_ptr<LQPColumnExpression>& dependent) const {
  std::vector<TableColumnID> candidates;
  visit_pqp(op, [&](const auto& current_op){
    switch (current_op->type()) {
      case OperatorType::Validate: return PQPVisitation::VisitInputs;
      default: return PQPVisitation::DoNotVisitInputs;
    }
  });

  return candidates;
  }

}  // namespace opossum
