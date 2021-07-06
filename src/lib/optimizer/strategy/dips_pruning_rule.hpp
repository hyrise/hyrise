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
  friend class DipsPruningRuleTest_RangeIntersectionTest_Test;
  friend class DipsPruningRuleTest_CalculatePrunedChunks_Test;
  friend class DipsPruningRuleTest_ApplyPruningSimple_Test;
  friend class DipsPruningRuleTest_ApplyPruning_Test;

 protected:
  std::vector<JoinMode> supported_join_types{JoinMode::Inner, JoinMode::Semi};  // extend if needed

  void _extend_pruned_chunks(const std::shared_ptr<StoredTableNode>& table_node,
                             const std::set<ChunkID>& pruned_chunk_ids) const;

  void _dips_pruning(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id,
                     std::shared_ptr<StoredTableNode> join_partner_table_node, ColumnID join_partner_column_id) const;

  void _visit_edge(DipsPruningGraphEdge& edge) const;

  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  template <typename COLUMN_TYPE>
  bool _range_intersect(std::pair<COLUMN_TYPE, COLUMN_TYPE> range_a,
                        std::pair<COLUMN_TYPE, COLUMN_TYPE> range_b) const {
    return !(((range_a.first < range_b.first) && (range_a.second < range_b.first)) ||
             ((range_a.first > range_b.second) && (range_a.second > range_b.second)));
  }

  template <typename COLUMN_TYPE>
  bool _range_prunable(std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> chunk_ranges,
                       std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>> join_ranges) const {
    for (auto join_range : join_ranges) {
      for (auto const& [_, ranges] : chunk_ranges) {
        for (auto range : ranges) {
          if (_range_intersect<COLUMN_TYPE>(join_range, range)) return false;
        }
      }
    }
    return true;
  }

  // We can only prune a chunk if no ranges of it are overlapping with any ranges in the chunks of the join table. To
  // check this we are iterating over every chunk and its ranges and comparing it with all ranges from the partner
  // table. If there is one case where the ranges intersect we skip the pruning of the chunk.
  template <typename COLUMN_TYPE>
  std::set<ChunkID> _calculate_pruned_chunks(
      std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> chunk_ranges,
      std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> join_chunk_ranges) const {
    std::set<ChunkID> pruned_chunk_ids;
    for (auto const& [join_chunk_id, join_ranges] : join_chunk_ranges) {
      if (_range_prunable(chunk_ranges, join_ranges)) {
        pruned_chunk_ids.insert(join_chunk_id);
      }
    }
    return pruned_chunk_ids;
  }

  // The algorithm works as follows:
  // 1. Get all chunk ids that are already pruned.
  // 2. Iterate overall not pruned chunks of the table.
  // 3. Get the segment statistic.
  // 4. Get the range statistic (for example: [(10, 400), (5000, 6000), ...]). If no range statistic exists use the
  //    min-max value instead.
  // 5. Return all ranges for the respective chunks.
  template <typename COLUMN_TYPE>
  std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> _get_not_pruned_range_statistics(
      const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id) const {
    using Range = std::pair<COLUMN_TYPE, COLUMN_TYPE>;
    using RangeList = std::vector<Range>;

    // For every non pruned chunk, we are saving the respective ranges.
    std::map<ChunkID, RangeList> ranges;

    auto pruned_chunks_ids = table_node->pruned_chunk_ids();
    auto table = Hyrise::get().storage_manager.get_table(table_node->table_name);

    for (ChunkID chunk_index = ChunkID{0}; chunk_index < table->chunk_count(); ++chunk_index) {
      if (std::find(pruned_chunks_ids.begin(), pruned_chunks_ids.end(), chunk_index) == pruned_chunks_ids.end()) {
        auto chunk_statistic = (*table->get_chunk(chunk_index)->pruning_statistics())[column_id];
        const auto segment_statistics =
            std::dynamic_pointer_cast<const AttributeStatistics<COLUMN_TYPE>>(chunk_statistic);

        Assert(segment_statistics, "expected AttributeStatistics");

        if constexpr (std::is_arithmetic_v<COLUMN_TYPE>) {
          if (segment_statistics->range_filter) {
            ranges.insert(std::pair<ChunkID, RangeList>(chunk_index, segment_statistics->range_filter->ranges));
          } else if (segment_statistics->dips_min_max_filter) {
            auto min = segment_statistics->dips_min_max_filter->min;
            auto max = segment_statistics->dips_min_max_filter->max;
            ranges.insert(std::pair<ChunkID, RangeList>(chunk_index, RangeList({Range(min, max)})));
          } else {
            ranges.insert(std::pair<ChunkID, RangeList>(chunk_index, RangeList()));
            // Note: if we don't do it, we assume, the chunk has been already pruned -> error
            continue;
          }
        } else if (segment_statistics->min_max_filter) {
          auto min = segment_statistics->min_max_filter->min;
          auto max = segment_statistics->min_max_filter->max;
          ranges.insert(std::pair<ChunkID, RangeList>(chunk_index, RangeList({Range(min, max)})));
        }
      }
    }

    return ranges;
  }
};

}  // namespace opossum
