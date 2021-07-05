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

void DipsPruningRule::_visit_edge(DipsPruningGraph::JoinGraphEdge& edge) const {
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

//   return stream;
// }

// First we are building a tree that represents the joins between the tables and its predicates. The nodes are the join
// tables and inside the edges are the join predicates. For example the following join procedure:
//            |><|
//           /    \
// A.a=C.d /        \ A.a=C.d
//       /            \
//     A              |><|
//                   /    \
//         B.b=C.d /        \ B.b=C.d
//               /            \
//             B                C
// will result in the following graph:
//                     C
//                   /    \
//         B.b=C.d /        \ A.a=C.d
//               /            \
//             B                A
// After that the basic procedure is to traverse the tree button up and then top down. In every visit we are running
// the dip pruning algorithm for the current node. The algorithm uses the predicates which are saved in the edge that
// is connecting the current node with its parent node. Inside the algorithm we check which chunks can be pruned in
// both the join tables.
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
