#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class ChunkStatistics;
class AbstractExpression;
class StoredTableNode;
class PredicateNode;
class Table;

/**
 * For a given LQP, this rule determines the set of chunks that can be pruned from a table.
 * To calculate the sets of pruned chunks, it analyzes the predicates of PredicateNodes in a given LQP.
 * The resulting pruning information is stored inside the StoredTableNode objects.
 */
class ChunkPruningRule : public AbstractRule {
 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  static std::set<ChunkID> _compute_exclude_list(const Table& table, const AbstractExpression& predicate,
                                                 const std::shared_ptr<StoredTableNode>& stored_table_node);

  // Check whether any of the statistics objects available for this Segment identify the predicate as prunable
  static bool _can_prune(const BaseAttributeStatistics& base_segment_statistics,
                         const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                         const std::optional<AllTypeVariant>& variant_value2);

  static bool _is_non_filtering_node(const AbstractLQPNode& node);

  static std::shared_ptr<TableStatistics> _prune_table_statistics(const TableStatistics& old_statistics,
                                                                  OperatorScanPredicate predicate,
                                                                  size_t num_rows_pruned);
};

}  // namespace opossum
