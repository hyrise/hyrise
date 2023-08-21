#pragma once

#include <memory>
#include <set>
#include <string>

#include "abstract_rule.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"
#include "utils/pruning_utils.hpp"

namespace hyrise {

/**
 * For a given LQP, this rule determines the set of chunks that can be pruned from a table. To calculate the sets of
 * pruned chunks, it analyzes the predicates of PredicateNodes in a given LQP. The resulting pruning information is
 * stored inside the StoredTableNode objects.
 * PredicateNodes may have predicates that contain the results of uncorrelated subqueries (e.g.,
 * `x = (SELECT MIN(y) FROM ...)`). We cannot prune chunks with these predicates during optimization since we do not
 * know the predicate value yet. However, we collect such predicates using subquery results that are safe to prune with
 * and also add this information to the StoredTableNode. During execution, the subquery might have already been executed
 * and the table can be pruned dynamically.
 */
class ChunkPruningRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  static std::vector<PredicatePruningChain> _find_predicate_pruning_chains_by_stored_table_node(
      const std::shared_ptr<StoredTableNode>& stored_table_node);

  static std::set<ChunkID> _intersect_chunk_ids(const std::vector<std::set<ChunkID>>& chunk_id_sets);

 private:
  /**
   * Caches intermediate results. Mutable because it needs to be called from the _apply_to_plan_without_subqueries
   * function, which is const.
   */
  mutable std::unordered_map<StoredTableNodePredicateNodePair, std::set<ChunkID>,
                             boost::hash<StoredTableNodePredicateNodePair>>
      _excluded_chunk_ids_by_predicate_node_cache;
};

}  // namespace hyrise
