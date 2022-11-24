#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"

namespace hyrise {

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
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  using PredicatePruningChain = std::vector<std::shared_ptr<PredicateNode>>;

  static std::vector<PredicatePruningChain> _find_predicate_pruning_chains_by_stored_table_node(
      const std::shared_ptr<StoredTableNode>& stored_table_node);

  std::set<ChunkID> _compute_exclude_list(const PredicatePruningChain& predicate_pruning_chain,
                                          const std::shared_ptr<StoredTableNode>& stored_table_node) const;

  // Check whether any of the statistics objects available for this Segment identify the predicate as prunable
  static bool _can_prune(const BaseAttributeStatistics& base_segment_statistics,
                         const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                         const std::optional<AllTypeVariant>& variant_value2);

  static std::shared_ptr<TableStatistics> _prune_table_statistics(const TableStatistics& old_statistics,
                                                                  OperatorScanPredicate predicate,
                                                                  size_t num_rows_pruned);

  static std::set<ChunkID> _intersect_chunk_ids(const std::vector<std::set<ChunkID>>& chunk_id_sets);

 private:
  /**
   * Caches intermediate results.
   * Mutable because it needs to be called from the _apply_to_plan_without_subqueries function, which is const.
   */
  using StoredTableNodePredicateNodePair = std::pair<std::shared_ptr<StoredTableNode>, std::shared_ptr<PredicateNode>>;
  mutable std::unordered_map<StoredTableNodePredicateNodePair, std::set<ChunkID>,
                             boost::hash<StoredTableNodePredicateNodePair>>
      _excluded_chunk_ids_by_predicate_node_cache;
};

}  // namespace hyrise
