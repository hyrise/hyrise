#pragma once

#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "dips_pruning_graph.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"

#include "abstract_rule.hpp"

#include "dips_pruning_rule.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "resolve_type.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

// std::ostream& operator<<(std::ostream& stream, const DipsJoinGraph join_graph);

class DipsPruningRule : public AbstractRule {
 protected:
  std::vector<JoinMode> supported_join_types{JoinMode::Inner, JoinMode::Semi};  // extend if needed
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  static void _dips_pruning(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id,
                            std::shared_ptr<StoredTableNode> join_partner_table_node, ColumnID join_partner_column_id);

  void _visit_edge(DipsPruningGraphEdge& edge) const;

  static void _extend_pruned_chunks(const std::shared_ptr<StoredTableNode>& table_node,
                                    const std::set<ChunkID>& pruned_chunk_ids);

  template <typename COLUMN_TYPE>
  static std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> _get_not_pruned_range_statistics(
      const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id);

  template <typename COLUMN_TYPE>
  static bool _range_intersect(std::pair<COLUMN_TYPE, COLUMN_TYPE> range_a,
                               std::pair<COLUMN_TYPE, COLUMN_TYPE> range_b);

  // We can only prune a chunk if no ranges of it are overlapping with any ranges in the chunks of the join table. To
  // check this we are iterating over every chunk and its ranges and comparing it with all ranges from the partner
  // table. If there is one case where the ranges intersect we skip the pruning of the chunk.
  template <typename COLUMN_TYPE>
  static std::set<ChunkID> _calculate_pruned_chunks(
      std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> base_chunk_ranges,
      std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> partner_chunk_ranges);
};

}  // namespace opossum
