#include "dips_pruning_rule.hpp"
#include <iostream>
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics.hpp"

// #include "hyrise.hpp"
// #include <fstream>

// TODO(Alex): Add comment which describes the basic idea of dips on an example with two tables.

namespace opossum {

void DipsPruningRule::_visit_edge(DipsPruningGraphEdge& edge) const {
  for (const auto& predicate : edge.predicates) {
    auto left_operand = predicate->left_operand();
    auto right_operand = predicate->right_operand();

    auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
    auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);

    Assert(left_lqp && right_lqp, "Expected LQPColumnExpression!");

    auto l = std::dynamic_pointer_cast<const StoredTableNode>(left_lqp->original_node.lock());
    auto r = std::dynamic_pointer_cast<const StoredTableNode>(right_lqp->original_node.lock());

    Assert(l && r, "Expected StoredTableNode");

    std::shared_ptr<StoredTableNode> left_stored_table_node = std::const_pointer_cast<StoredTableNode>(l);
    std::shared_ptr<StoredTableNode> right_stored_table_node = std::const_pointer_cast<StoredTableNode>(r);

    if (!left_stored_table_node || !right_stored_table_node) {
      return;
    }

    // LEFT -> RIGHT
    _dips_pruning(left_stored_table_node, left_lqp->original_column_id, right_stored_table_node,
                  right_lqp->original_column_id);

    // RIGHT -> LEFT
    _dips_pruning(right_stored_table_node, right_lqp->original_column_id, left_stored_table_node,
                  left_lqp->original_column_id);
  }
}

void DipsPruningRule::_extend_pruned_chunks(const std::shared_ptr<StoredTableNode>& table_node,
                                            const std::set<ChunkID>& pruned_chunk_ids) {
  const auto& already_pruned_chunk_ids = table_node->pruned_chunk_ids();

  if (!already_pruned_chunk_ids.empty()) {
    std::vector<ChunkID> union_values;
    std::set_union(already_pruned_chunk_ids.begin(), already_pruned_chunk_ids.end(), pruned_chunk_ids.begin(),
                   pruned_chunk_ids.end(), std::back_inserter(union_values));
    table_node->set_pruned_chunk_ids(union_values);
  } else {
    table_node->set_pruned_chunk_ids(std::vector<ChunkID>(pruned_chunk_ids.begin(), pruned_chunk_ids.end()));
  }
}

void DipsPruningRule::_dips_pruning(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id,
                                    std::shared_ptr<StoredTableNode> join_partner_table_node,
                                    ColumnID join_partner_column_id) {
  auto table = Hyrise::get().storage_manager.get_table(table_node->table_name);

  resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    // TODO(somebody): check if pointers would be more efficient
    auto base_ranges = _get_not_pruned_range_statistics<ColumnDataType>(table_node, column_id);
    auto partner_ranges =
        _get_not_pruned_range_statistics<ColumnDataType>(join_partner_table_node, join_partner_column_id);
    auto pruned_chunks = _calculate_pruned_chunks<ColumnDataType>(base_ranges, partner_ranges);
    _extend_pruned_chunks(join_partner_table_node, pruned_chunks);
  });
}

// The algorithm works as follows:
// 1. Get all chunk ids that are already pruned.
// 2. Iterate overall not pruned chunks of the table.
// 3. Get the segment statistic.
// 4. Get the range statistic (for example: [(10, 400), (5000, 6000), ...]). If no range statistic exists use the
//    min-max value instead.
// 5. Return all ranges for the respective chunks.
template <typename COLUMN_TYPE>
std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> DipsPruningRule::_get_not_pruned_range_statistics(
    const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id) {
  // For every non pruned chunk, we are saving the respective ranges.
  std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> ranges;

  auto pruned_chunks_ids = table_node->pruned_chunk_ids();  // const std::vector<ChunkID>&
  auto table = Hyrise::get().storage_manager.get_table(table_node->table_name);

  for (ChunkID chunk_index = ChunkID{0}; chunk_index < table->chunk_count(); ++chunk_index) {
    if (std::find(pruned_chunks_ids.begin(), pruned_chunks_ids.end(), chunk_index) == pruned_chunks_ids.end()) {
      auto chunk_statistic = (*table->get_chunk(chunk_index)->pruning_statistics())[column_id];
      const auto segment_statistics =
          std::dynamic_pointer_cast<const AttributeStatistics<COLUMN_TYPE>>(chunk_statistic);

      Assert(segment_statistics, "expected AttributeStatistics");

      if constexpr (std::is_arithmetic_v<COLUMN_TYPE>) {
        if (segment_statistics->range_filter) {  // false if all values in the chunk are NULL
          ranges.insert(std::pair<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>(
              chunk_index, segment_statistics->range_filter->ranges));
        } else {
          if (segment_statistics->dips_min_max_filter) {
            ranges.insert(std::pair<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>(
                chunk_index,
                std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>({std::pair<COLUMN_TYPE, COLUMN_TYPE>(
                    segment_statistics->dips_min_max_filter->min, segment_statistics->dips_min_max_filter->max)})));
          } else {
            ranges.insert(std::pair<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>(
                chunk_index, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>()));
            // Note: if we don't do it, we assume, the chunk has been already pruned -> error
            continue;
          }

          // RangeFilters contain all the information stored in a MinMaxFilter. There is no point in having both.
          DebugAssert(!segment_statistics->min_max_filter,
                      "Segment should not have a MinMaxFilter and a RangeFilter at the same time");
        }
      }

      // We should  not use insert. Instead we should manually check if there is already an entry.
      if (segment_statistics->min_max_filter) {
        ranges.insert(std::pair<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>(
            chunk_index, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>({std::pair<COLUMN_TYPE, COLUMN_TYPE>(
                             segment_statistics->min_max_filter->min, segment_statistics->min_max_filter->max)})));
      }
    }
  }

  return ranges;
}

template <typename COLUMN_TYPE>
bool DipsPruningRule::_range_intersect(std::pair<COLUMN_TYPE, COLUMN_TYPE> range_a,
                                       std::pair<COLUMN_TYPE, COLUMN_TYPE> range_b) {
  return !(((range_a.first < range_b.first) && (range_a.second < range_b.first)) ||
           ((range_a.first > range_b.second) && (range_a.second > range_b.second)));
}

// We can only prune a chunk if no ranges of it are overlapping with any ranges in the chunks of the join table. To
// check this we are iterating over every chunk and its ranges and comparing it with all ranges from the partner
// table. If there is one case where the ranges intersect we skip the pruning of the chunk.
template <typename COLUMN_TYPE>
std::set<ChunkID> DipsPruningRule::_calculate_pruned_chunks(
    std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> base_chunk_ranges,
    std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> partner_chunk_ranges) {
  /* Calculate the chunks ids of the partner table which can be pruned (based on base_chunk_ranges) */
  std::set<ChunkID> pruned_chunk_ids;

  for (auto const& [partner_chunk_id, partner_ranges] : partner_chunk_ranges) {
    bool can_be_pruned = true;

    for (auto partner_range : partner_ranges) {
      if (!can_be_pruned) break;
      for (auto const& [base_chunk_id, base_ranges] : base_chunk_ranges) {
        if (!can_be_pruned) break;
        for (auto base_range : base_ranges) {
          if (_range_intersect<COLUMN_TYPE>(partner_range, base_range)) {
            can_be_pruned = false;
            break;
          }
        }
      }
    }
    if (can_be_pruned) {
      pruned_chunk_ids.insert(partner_chunk_id);
    }
  }

  return pruned_chunk_ids;
}

// std::ostream& operator<<(std::ostream& stream, const DipsJoinGraph& join_graph) {
//   stream << "==== Vertices ====" << std::endl;
//   if (join_graph.nodes.empty()) {
//     stream << "<none>" << std::endl;
//   } else {
//     for (const auto& node : join_graph.nodes) {
//       stream << node->table_node->description() << std::endl;
//       stream << "      ==== Adress ====" << std::endl;
//       stream << "          " << node << std::endl;
//       stream << "      ==== Parent ====" << std::endl;
//       stream << "          " << node->parent << std::endl;
//       stream << "      ==== Children ====" << std::endl;
//       for (const auto& child : node->children) {
//         stream << "          " << child << std::endl;
//       }

//       stream << "      ==== Edges ====" << std::endl;
//       for (const auto& edge : node->edges) {
//         stream << "      " << edge->partner_node->table_node->description() << std::endl;
//         stream << "            ==== Predicates ====" << std::endl;
//         for (const auto& predicate : edge->predicates) {
//           stream << "            " << predicate->description(AbstractExpression::DescriptionMode::ColumnName)
//                  << std::endl;
//         }
//       }
//     }
//   }
//
//   return stream;
// }

/**
*  First we are building a tree that represents the joins between the tables and its predicates. The nodes are the join
*  tables and inside the edges are the join predicates. For example the following join procedure:
*             |><|
*            /    \
*  A.a=C.d /        \ A.a=C.d
*        /            \
*      A              |><|
*                    /    \
*          B.b=C.d /        \ B.b=C.d
*                /            \
*              B                C
*  will result in the following graph:
*                      C
*                    /    \
*          B.b=C.d /        \ A.a=C.d
*                /            \
*              B                A
*  After that the basic procedure is to traverse the tree button up and then top down. In every visit we are running
*  the dip pruning algorithm for the current node. The algorithm uses the predicates which are saved in the edge that
*  is connecting the current node with its parent node. Inside the algorithm we check which chunks can be pruned in
*  both the join tables.
*/
void DipsPruningRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  auto graph = DipsPruningGraph{};
  graph.build_graph(lqp_root);
  if (graph.empty()) {
    return;
  }

  if (graph.is_tree()) {
    for (auto& edge : graph.bottom_up_traversal()) {
      _visit_edge(edge);
    }
    for (auto& edge : graph.top_down_traversal()) {
      _visit_edge(edge);
    }
  } else {
    // Assumption: Hyrise handles cycles itself
  }
}

}  // namespace opossum
