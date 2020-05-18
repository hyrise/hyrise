#pragma once

#include <unordered_map>
#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

// The PQP sub plan memoization for StoredTableNodes is sensitive to the node's table name, set of pruned chunks and
// set of pruned columns. Consequently, for multiple nodes with the same table name, same pruned chunks and same pruned
// columns, only one GetTable operator is created and executed. In some queries, the ColumnPruningRule and
// ChunkPruningRule provide an LQP with multiple StoredTableNodes where the table names and sets of pruned chunks are
// equal but the sets of pruned columns are different. This leads to the creation and execution of different GetTable
// operators instead of only one operator.
//
// This rule identifies StoredTableNodes that differ only in the pruned columns. It then intersects the lists of pruned
// columns. While this means that some columns are left unpruned, it makes the job easier for the memoization in the
// LQPTranslator. In our experiments, this has led to significant performance improvements and negligible reductions.
//
// Example: LQP sub plan before executing the StoredTableColumnAlignmentRule
//
//                      +------------------------+
//                      | Join (1)               |
//                      | Mode: Inner            |
//                      | predicate: Li_a = Pa_a |
//                      +------------------------+
//                         /                \
//       +------------------------+     +------------------------+
//       | Join (2)               |     | Join (3)               |
//       | Mode: Semi             |     | Mode: Semi             |
//       | predicate: Li_a = Pa_a |     | predicate: Li_a = Pa_a |
//       +------------------------+     +------------------------+
//                   /          \           /           \
// +---------------------+  +-----------------------+  +-----------------------+
// | StoredTable (1)     |  | StoredTable (2)       |  | StoredTable (3)       |
// | table name:     Li  |  | table name:     Pa    |  | table name:     Li    |
// | pruned chunks:  { } |  | pruned chunks:  {1,2} |  | pruned chunks:  { }   |
// | pruned columns: {3} |  | pruned columns: { }   |  | pruned columns: {3,4} |
// +---------------------+  +-----------------------+  +-----------------------+
//
// Join (2) and Join (3) in the above sub plan have the same join mode and the same join predicate. Both use the same
// input tables (Li and Pa). However, the input StoredTable (1) and (3) for the table Li are different since the set of
// pruned columns are not equal. When an LQPNode n1 is translated, the PQP sub plan memoization reuses an operator that
// was already created for another LQPNode n2 that is semantically equal to n1. The equality comparison of StoredTable
// nodes is sensitive to the table name, pruned chunks and pruned columns. Consequently, Stored Table nodes (1) and (3)
// unequal and two different GetTable operators would be created in the PQP. For LQPNode equality comparisons, the
// input nodes are generally relevant. Joins (2) and (3) are not semantically equal since they have semantically
// unequal input StoredTable nodes for table "Li". As a result, the LQPTranslator would create two Join operators.
//
// In order to create and execute less operators and therefore to reduce the execution time of the resulting PQP, this
// rule would align the pruned columns of StoredTableNode (3): The pruned columns {3,4} are a super set of the set of
// pruned columns for StoredTable node (1). Reducing the set of pruned columns to {3} would:
// - still guarantee that all necessary columns for nodes on higher levels in the overall LQP are still available and
// - make StoredTable node (1) and StoredTable node (3) semantically equal. Therefore only one GetTable operator for
//   both nodes would be created and executed. Additionally, Join (2) and Join (3) would also be semantically equal
//   and only one Join operator for both nodes would be created and executed.
class StoredTableColumnAlignmentRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;
};

}  // namespace opossum
