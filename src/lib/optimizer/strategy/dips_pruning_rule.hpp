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
class DipsJoinGraphEdge;
class DipsJoinGraphNode;
class DipsJoinGraph;

class DipsJoinGraphEdge {
 public:
  std::shared_ptr<DipsJoinGraphNode> partner_node;
  std::vector<std::shared_ptr<BinaryPredicateExpression>> predicates;

  explicit DipsJoinGraphEdge(std::shared_ptr<DipsJoinGraphNode> edge_partner_node) {
    this->partner_node = edge_partner_node;
    this->predicates = std::vector<std::shared_ptr<BinaryPredicateExpression>>();
  }

  void append_predicate(std::shared_ptr<BinaryPredicateExpression> predicate) {
    // TODO(somebody): remove search when implementation "visit single node in LQP only once" is done
    if (std::find(predicates.begin(), predicates.end(), predicate) == predicates.end()) {
      predicates.push_back(predicate);
    }
  }
};

class DipsJoinGraphNode {
 public:
  std::vector<std::shared_ptr<DipsJoinGraphNode>> children = std::vector<std::shared_ptr<DipsJoinGraphNode>>();
  std::shared_ptr<DipsJoinGraphNode> parent;
  std::shared_ptr<StoredTableNode> table_node;
  std::vector<std::shared_ptr<DipsJoinGraphEdge>> edges;

  explicit DipsJoinGraphNode(std::shared_ptr<StoredTableNode> stored_table_node) {
    this->table_node = stored_table_node;
  }

  std::shared_ptr<DipsJoinGraphEdge> get_edge_for_table(std::shared_ptr<DipsJoinGraphNode> partner_table_node) {
    for (auto edge : edges) {
      if (edge->partner_node == partner_table_node) {
        return edge;
      }
    }
    std::shared_ptr<DipsJoinGraphEdge> edge = std::make_shared<DipsJoinGraphEdge>(partner_table_node);
    edges.push_back(edge);
    return edge;
  }
};

class DipsJoinGraph {
 public:
  std::vector<std::shared_ptr<DipsJoinGraphNode>> nodes;

  std::shared_ptr<DipsJoinGraphNode> get_node_for_table(std::shared_ptr<StoredTableNode> table_node) {
    for (auto graph_node : nodes) {
      if (graph_node->table_node == table_node) {
        return graph_node;
      }
    }
    std::shared_ptr<DipsJoinGraphNode> graph_node = std::make_shared<DipsJoinGraphNode>(table_node);
    nodes.push_back(graph_node);
    return graph_node;
  }

  bool is_empty() {
    if (nodes.size() == 0) {
      return true;
    }
    return false;
  }

  bool is_tree() {
    if (nodes.size() == 0) {
      return true;
    }
    std::shared_ptr<std::vector<std::shared_ptr<DipsJoinGraphNode>>> visited_nodes =
        std::make_shared<std::vector<std::shared_ptr<DipsJoinGraphNode>>>();
    return _is_tree_dfs(nullptr, nodes[0], visited_nodes);
  }

  void set_root(std::shared_ptr<DipsJoinGraphNode> root) { _set_root_dfs(nullptr, root); }

 private:
  bool _is_tree_dfs(std::shared_ptr<DipsJoinGraphNode> parent_node, std::shared_ptr<DipsJoinGraphNode> node,
                    std::shared_ptr<std::vector<std::shared_ptr<DipsJoinGraphNode>>> visited_nodes) {
    visited_nodes->push_back(node);
    for (auto edge : node->edges) {
      if (edge->partner_node != parent_node) {
        if (std::find(visited_nodes->begin(), visited_nodes->end(), edge->partner_node) == visited_nodes->end()) {
          if (!_is_tree_dfs(node, edge->partner_node, visited_nodes)) {
            return false;
          }
        } else {
          return false;
        }
      }
    }
    return true;
  }
  void _set_root_dfs(std::shared_ptr<DipsJoinGraphNode> parent_node, std::shared_ptr<DipsJoinGraphNode> node) {
    node->parent = parent_node;

    for (auto edge : node->edges) {
      if (edge->partner_node != parent_node) {
        node->children.push_back(edge->partner_node);
        _set_root_dfs(node, edge->partner_node);
      }
    }
  }
};

std::ostream& operator<<(std::ostream& stream, const DipsJoinGraph join_graph);

class DipsPruningRule : public AbstractRule {
 public:
  void apply_to_plan(const std::shared_ptr<LogicalPlanRootNode>& node) const override;

 protected:
  std::vector<JoinMode> supported_join_types{JoinMode::Inner, JoinMode::Semi};  // extend if needed
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  static void _dips_pruning(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id,
                            std::shared_ptr<StoredTableNode> join_partner_table_node, ColumnID join_partner_column_id);

  void _build_join_graph(const std::shared_ptr<AbstractLQPNode>& node,
                         const std::shared_ptr<DipsJoinGraph>& join_graph) const;

  static void _extend_pruned_chunks(const std::shared_ptr<StoredTableNode>& table_node,
                                    const std::set<ChunkID>& pruned_chunk_ids);
  void _top_down_dip_traversal(const std::shared_ptr<DipsJoinGraphNode>& node) const;
  void _bottom_up_dip_traversal(const std::shared_ptr<DipsJoinGraphNode>& node) const;

  template <typename COLUMN_TYPE>
  static std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> _get_not_pruned_range_statistics(
      const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id) {
    /* For every non pruned chunk, return its ranges for the given attribute (segment) */
    std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> ranges;

    auto pruned_chunks_ids = table_node->pruned_chunk_ids();  // const std::vector<ChunkID>&
    auto table = Hyrise::get().storage_manager.get_table(table_node->table_name);

    for (ChunkID chunk_index = ChunkID{0}; chunk_index < table->chunk_count(); ++chunk_index) {
      if (std::find(pruned_chunks_ids.begin(), pruned_chunks_ids.end(), chunk_index) == pruned_chunks_ids.end()) {
        auto chunk_statistic = (*table->get_chunk(chunk_index)->pruning_statistics())[column_id];
        const auto segment_statistics =
            std::dynamic_pointer_cast<const AttributeStatistics<COLUMN_TYPE>>(chunk_statistic);

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

