#include "index_scan_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_parameter_variant.hpp"
#include "cost_estimation/abstract_cost_estimator.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// Only if we expect num_output_rows <= num_input_rows * selectivity_threshold, the ScanType can be set to IndexScan.
// This threshold is kind of arbitrarily chosen, but the following paper suggests something similar: "Access Path
// Selection in Main-Memory Optimized Data Systems: Should I Scan or Should I Probe?"
constexpr float INDEX_SCAN_SELECTIVITY_THRESHOLD = 0.01f;

// Only if the number of input rows exceeds num_input_rows, the ScanType can be set to IndexScan. The threshold is taken
// from: "Fast Lookups for In-Memory Column Stores: Group-Key Indices, Lookup and Maintenance."
constexpr float INDEX_SCAN_ROW_COUNT_THRESHOLD = 1000.0f;

bool is_single_column_index(const TableIndexStatistics& index_statistics) {
  return index_statistics.column_ids.size() == 1;
}

bool is_index_scan_applicable(const TableIndexStatistics& index_statistics,
                              const std::shared_ptr<PredicateNode>& predicate_node,
                              const std::shared_ptr<AbstractCostEstimator>& cost_estimator) {
  if (!is_single_column_index(index_statistics)) {
    return false;
  }

  const auto operator_predicates =
      OperatorScanPredicate::from_expression(*predicate_node->predicate(), *predicate_node);
  if (!operator_predicates || operator_predicates->size() != 1) {
    return false;
  }

  const auto& operator_predicate = (*operator_predicates)[0];

  // Currently, we do not support two-column predicates
  if (is_column_id(operator_predicate.value)) {
    return false;
  }

  if (index_statistics.column_ids[0] != operator_predicate.column_id) {
    return false;
  }

  // There is no conceptual limitation to this rule with other predicate conditions, but Hyrise's secondary indexes are
  // hash-based and thus lack support for other predicate conditions.
  if (operator_predicate.predicate_condition != PredicateCondition::Equals &&
      operator_predicate.predicate_condition != PredicateCondition::NotEquals) {
    return false;
  }

  const auto row_count_table =
      cost_estimator->cardinality_estimator->estimate_cardinality(predicate_node->left_input());
  if (row_count_table < INDEX_SCAN_ROW_COUNT_THRESHOLD) {
    return false;
  }

  const auto row_count_predicate = cost_estimator->cardinality_estimator->estimate_cardinality(predicate_node);
  const float selectivity = row_count_predicate / row_count_table;

  return selectivity <= INDEX_SCAN_SELECTIVITY_THRESHOLD;
}

}  // namespace

namespace hyrise {

std::string IndexScanRule::name() const {
  static const auto name = std::string{"IndexScanRule"};
  return name;
}

void IndexScanRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  DebugAssert(cost_estimator, "IndexScanRule requires cost estimator to be set.");
  Assert(lqp_root->type == LQPNodeType::Root, "ExpressionReductionRule needs root to hold onto.");

  visit_lqp(lqp_root, [&](const auto& node) {
    if (node->type == LQPNodeType::Predicate) {
      const auto& child = node->left_input();

      if (child->type == LQPNodeType::StoredTable) {
        const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
        const auto stored_table_node = std::static_pointer_cast<StoredTableNode>(child);

        const auto& indexes_statistics = stored_table_node->table_indexes_statistics();
        for (const auto& index_statistics : indexes_statistics) {
          if (is_index_scan_applicable(index_statistics, predicate_node, cost_estimator)) {
            predicate_node->scan_type = ScanType::IndexScan;
          }
        }
      }
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace hyrise
