#pragma once

#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

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

struct Graph {
  friend class DipsPruningRuleTest_BuildJoinGraph_Test;
  friend class DipsPruningRuleTest_JoinGraphIsTree_Test;
  friend class DipsPruningRuleTest_DipsJoinGraphIsNoTree_Test;
  friend class DipsPruningRuleTest_DipsJoinGraphTraversal_Test;
  using JoinGraphVertexSet = std::set<size_t>;

  struct JoinGraphEdge {
   public:

    explicit JoinGraphEdge(JoinGraphVertexSet init_vertex_set, std::shared_ptr<BinaryPredicateExpression> predicate)  : vertex_set(init_vertex_set) {
       predicates.push_back(predicate);
    }

    void append_predicate(std::shared_ptr<BinaryPredicateExpression> predicate) {
      // TODO(somebody): remove search when implementation "visit single node in LQP only once" is done
      if (std::find(predicates.begin(), predicates.end(), predicate) == predicates.end()) {
        predicates.push_back(predicate);
      }
    }

    bool connects_vertex(size_t vertex) {
      return vertex_set.find(vertex) != vertex_set.end();
    }

    size_t neighbour(size_t vertex) {
      for(auto neighbour : vertex_set) {
        if (neighbour != vertex) {
          return neighbour;
        }
      }
      Assert(false, "There always should be a neighbor");
    }

    JoinGraphVertexSet vertex_set;
    std::vector<std::shared_ptr<BinaryPredicateExpression>> predicates;
  };

  // To be able to push dips through joins we first need to construct a graph on which we can execute the main algorithm.
  //  We are doing this by recursively traversing over the LQP graph. In every visit of a node the following steps are
  // executed:
  // 1. Check that the currently visited node is a join node.
  // 2. Get the join predicates
  // 3. Check that the left and right operands are LQPColumnExpression.
  // 4. Get each of the associated StoredTableNode of the left and right expressions.
  // 5. Add both of the storage nodes to the graph (if they are not in it) and connect them with edges (if they are not
  // connected).
  // 6. Add the predicates to the associated edges.
  void build_graph(const std::shared_ptr<AbstractLQPNode>& node){
  // Why do we exit in this cases ?
  if (node->type == LQPNodeType::Union || node->type == LQPNodeType::Intersect || node->type == LQPNodeType::Except) {
    return;
  }

  if (node->left_input()) build_graph(node->left_input());
  if (node->right_input()) build_graph(node->right_input());

  // This rule only supports the inner and semi join
  if (node->type == LQPNodeType::Join) {
    if (std::find(supported_join_types.begin(), supported_join_types.end(),
                  std::dynamic_pointer_cast<JoinNode>(node)->join_mode) == supported_join_types.end()) {
      return;
    }
    const auto& join_node = static_cast<JoinNode&>(*node);
    const auto& join_predicates = join_node.join_predicates();

    for (const auto& predicate : join_predicates) {
      // Why do we need to cast the predicates to binary predicate expressions?
      std::shared_ptr<BinaryPredicateExpression> binary_predicate =
          std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);

      Assert(binary_predicate, "Expected BinaryPredicateExpression!");

      // We are only interested in equal predicate conditions (The dibs rule is only working with equal predicates)
      if (binary_predicate->predicate_condition != PredicateCondition::Equals) {
        continue;
      }

      auto left_operand = binary_predicate->left_operand();
      auto right_operand = binary_predicate->right_operand();

      auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
      auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);

      // We need to check that the type is LQPColumn
      if (!left_lqp || !right_lqp) {
        continue;
      }

      auto l = std::dynamic_pointer_cast<const StoredTableNode>(left_lqp->original_node.lock());
      auto r = std::dynamic_pointer_cast<const StoredTableNode>(right_lqp->original_node.lock());

      Assert(l && r, "Expected StoredTableNode");

      std::shared_ptr<StoredTableNode> left_stored_table_node = std::const_pointer_cast<StoredTableNode>(l);
      std::shared_ptr<StoredTableNode> right_stored_table_node = std::const_pointer_cast<StoredTableNode>(r);

      // access join graph nodes (every storage table note is represented inside the join graph)
      auto left_join_graph_node = _get_vertex(left_stored_table_node);
      auto right_join_graph_node = _get_vertex(right_stored_table_node);

      auto vertex_set = _get_vertex_set(left_join_graph_node, right_join_graph_node);

      _add_edge(vertex_set, binary_predicate);
    }
  }
}

std::vector<JoinGraphEdge> top_down_traversal() {
  std::vector<JoinGraphEdge> traversal_order{};
  std::set<size_t> visited{};
  _top_down_traversal_visit(0, traversal_order, visited);
  return traversal_order;
}

std::vector<JoinGraphEdge> bottom_up_traversal() {
  std::vector<JoinGraphEdge> traversal_order{};
  std::set<size_t> visited{};
  _bottom_up_traversal_visit(0, traversal_order, visited);
  return traversal_order;
}

bool is_tree() {
  std::set<size_t> visited{};
  return _is_tree_visit(0, 0, visited);
}

bool empty() {
  return vertices.size() == 0;
}

private:
  size_t _get_vertex(std::shared_ptr<StoredTableNode> table_node) {
    auto it = std::find(vertices.begin(), vertices.end(), table_node);
    if (it != vertices.end()) {
      return it - vertices.begin();
    }
    vertices.push_back(table_node);
    return vertices.size() - 1;
  }

  JoinGraphVertexSet _get_vertex_set(size_t noda_a, size_t noda_b){
    Assert((noda_a < vertices.size() || noda_b  < vertices.size()), "Nodes should exist in graph");

    return JoinGraphVertexSet{noda_a, noda_b};
  }

  void _add_edge(JoinGraphVertexSet vertex_set, std::shared_ptr<BinaryPredicateExpression> predicate) {
     for (auto& edge : edges) {
      if (vertex_set == edge.vertex_set) {
        edge.append_predicate(predicate);
        return;
      }
     }
      edges.emplace_back(vertex_set, predicate);
  }

  bool _is_tree_visit(size_t current_node, size_t parrent, std::set<size_t>& visited) {
    visited.insert(current_node);

    for (auto& edge : edges) {
      if (edge.connects_vertex(current_node)) {
        auto neighbour = edge.neighbour(current_node);
        // We do not want to go back to the parent node.
        if (neighbour == parrent) continue;
        if (visited.find(neighbour) != visited.end()) return false;
        if (!_is_tree_visit(neighbour, current_node, visited)) return false;
      }
    }
    return true;
  }

  void _top_down_traversal_visit(size_t current_node, std::vector<JoinGraphEdge>& traversal_order, std::set<size_t>& visited){
    visited.insert(current_node);
    for (auto& edge : edges) {
      if (edge.connects_vertex(current_node)) {
        auto neighbour = edge.neighbour(current_node);
        // We do not want to go back to the parent node.
        if (visited.find(neighbour) != visited.end()) continue;
        traversal_order.push_back(edge);
        _top_down_traversal_visit(neighbour, traversal_order, visited);
      }
    }
  }

  void _bottom_up_traversal_visit(size_t current_node, std::vector<JoinGraphEdge>& traversal_order, std::set<size_t>& visited) {
    visited.insert(current_node);
    // TODO: Fix Hacky solution
    auto parent_edge = edges[0];
    for (auto& edge : edges) {
      if (edge.connects_vertex(current_node)) {
        auto neighbour = edge.neighbour(current_node);
        if (visited.find(neighbour) != visited.end()) {
          parent_edge = edge;
          continue;
        }
        _bottom_up_traversal_visit(neighbour, traversal_order, visited);
      }
    }
    traversal_order.push_back(parent_edge);
  }


  std::vector<JoinMode> supported_join_types{JoinMode::Inner, JoinMode::Semi};
  std::vector<std::shared_ptr<StoredTableNode>> vertices;
  std::vector<JoinGraphEdge> edges;
};

// std::ostream& operator<<(std::ostream& stream, const DipsJoinGraph join_graph);

class DipsPruningRule : public AbstractRule {
 protected:
  std::vector<JoinMode> supported_join_types{JoinMode::Inner, JoinMode::Semi};  // extend if needed
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  static void _dips_pruning(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id,
                            std::shared_ptr<StoredTableNode> join_partner_table_node, ColumnID join_partner_column_id);

  void _visit_edge(Graph::JoinGraphEdge& edge) const;

  static void _extend_pruned_chunks(const std::shared_ptr<StoredTableNode>& table_node,
                                    const std::set<ChunkID>& pruned_chunk_ids);

  // The algorithm works as follows:
  // 1. Get all chunk ids that are already pruned.
  // 2. Iterate overall not pruned chunks of the table.
  // 3. Get the segment statistic.
  // 4. Get the range statistic (for example: [(10, 400), (5000, 6000), ...]). If no range statistic exists use the
  //    min-max value instead.
  // 5. Return all ranges for the respective chunks.
  template <typename COLUMN_TYPE>
  static std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> _get_not_pruned_range_statistics(
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
  static bool _range_intersect(std::pair<COLUMN_TYPE, COLUMN_TYPE> range_a,
                               std::pair<COLUMN_TYPE, COLUMN_TYPE> range_b) {
    return !(((range_a.first < range_b.first) && (range_a.second < range_b.first)) ||
             ((range_a.first > range_b.second) && (range_a.second > range_b.second)));
  }

  // We can only prune a chunk if no ranges of it are overlapping with any ranges in the chunks of the join table. To
  // check this we are iterating over every chunk and its ranges and comparing it with all ranges from the partner
  // table. If there is one case where the ranges intersect we skip the pruning of the chunk.
  template <typename COLUMN_TYPE>
  static std::set<ChunkID> _calculate_pruned_chunks(
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
};

}  // namespace opossum
