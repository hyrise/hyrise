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
 public:
  static bool _is_non_filtering_node(const AbstractLQPNode& node);  // TODO outsource to LQPUtils?

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  std::set<ChunkID> _compute_exclude_list(const Table& table,
                                          const std::vector<std::shared_ptr<PredicateNode>>& predicate_chain,
                                          const std::shared_ptr<StoredTableNode>& stored_table_node) const;

  // Check whether any of the statistics objects available for this Segment identify the predicate as prunable
  static bool _can_prune(const BaseAttributeStatistics& base_segment_statistics,
                         const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                         const std::optional<AllTypeVariant>& variant_value2);

  static std::shared_ptr<TableStatistics> _prune_table_statistics(const TableStatistics& old_statistics,
                                                                  OperatorScanPredicate predicate,
                                                                  size_t num_rows_pruned);

  static std::set<ChunkID> intersect_chunk_ids(const std::vector<std::set<ChunkID>>& chunk_id_sets);

  static std::vector<std::vector<std::shared_ptr<PredicateNode>>> find_predicate_chains_recursively(
      std::shared_ptr<StoredTableNode> stored_table_node, std::shared_ptr<AbstractLQPNode> node,
      std::vector<std::shared_ptr<PredicateNode>> current_predicate_chain = {});

 private:
  // Caches intermediate results
  mutable std::unordered_map<std::shared_ptr<PredicateNode>, std::set<ChunkID>> _excluded_chunk_ids_by_predicate_node;
};

}  // namespace opossum
