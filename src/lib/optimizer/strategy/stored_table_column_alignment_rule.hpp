#pragma once

#include <unordered_map>
#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

// The PQP sub plan memoization for StoredTableNodes is sensitive to the node's table name, set of pruned chunks and
// set of pruned columns. Consequently, for multiple nodes with the same table name, chunk and column pruning, only one
// GetTable operator is created and executed. In some queries, the ColumnPruningRule and ChunkPruningRule provide an
// LQP with multiple StoredTableNodes where the table names and sets of pruned chunks are equal but the sets of pruned
// columns are different. This leads to the creation and execution of different GetTable operators.
//
// This rule identifies StoredTableNodes with equal table names and sets of pruned chunks and aligns the pruned columns
// so that the sets of pruned columns are equal which means that the PQP sub plan memoization is effective for theses
// StoredTableNodes and only one GetTable operator is created.
class StoredTableColumnAlignmentRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;
};

}  // namespace opossum
