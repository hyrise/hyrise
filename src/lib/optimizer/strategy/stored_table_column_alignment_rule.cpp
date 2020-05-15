#include "optimizer/strategy/stored_table_column_alignment_rule.hpp"

#include <algorithm>
#include <memory>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

void StoredTableColumnAlignmentRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  StoredTableNodeUnorderedMap<std::vector<std::shared_ptr<StoredTableNode>>> gathered_stored_table_nodes_for_node;
  StoredTableNodeUnorderedMap<std::vector<ColumnID>> aligned_pruned_column_ids_for_node;
  recursively_gather_stored_table_nodes(root, gathered_stored_table_nodes_for_node, aligned_pruned_column_ids_for_node);
  // iterate over groups of StoredTableNode and set the maximum superset of columns for each StoredTableNode in each
  // StoredTableNode group. A group is characterized by the table name and the pruned chunk ids.
  for (const auto& node_nodes_pair : gathered_stored_table_nodes_for_node) {
    for (const auto& stored_table_node : node_nodes_pair.second) {
      stored_table_node->set_pruned_column_ids(aligned_pruned_column_ids_for_node[stored_table_node]);
    }
  }
}

void StoredTableColumnAlignmentRule::recursively_gather_stored_table_nodes(
    const std::shared_ptr<AbstractLQPNode>& node,
    StoredTableNodeUnorderedMap<std::vector<std::shared_ptr<StoredTableNode>>>& gathered_stored_table_nodes_for_node,
    StoredTableNodeUnorderedMap<std::vector<ColumnID>>& aligned_pruned_column_ids_for_node) const {
  const auto& left_input = node->left_input();
  const auto& right_input = node->right_input();

  if (left_input) {
    recursively_gather_stored_table_nodes(left_input, gathered_stored_table_nodes_for_node,
                                          aligned_pruned_column_ids_for_node);
  }
  if (right_input) {
    recursively_gather_stored_table_nodes(right_input, gathered_stored_table_nodes_for_node,
                                          aligned_pruned_column_ids_for_node);
  }
  if (node->type == LQPNodeType::StoredTable) {
    const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);

    auto& gathered_stored_table_nodes = gathered_stored_table_nodes_for_node[stored_table_node];
    gathered_stored_table_nodes.emplace_back(stored_table_node);

    // update maximum superset of pruned column ids for a StoredTableNodes with the same table name and same set of
    // pruned chunk ids
    if (aligned_pruned_column_ids_for_node.contains(stored_table_node)) {
      auto& aligned_pruned_column_ids = aligned_pruned_column_ids_for_node[stored_table_node];
      if (aligned_pruned_column_ids.empty()) {
      }
      std::vector<ColumnID> updated_pruned_column_ids{};
      updated_pruned_column_ids.reserve(aligned_pruned_column_ids.size() +
                                        stored_table_node->pruned_column_ids().size());
      std::set_intersection(aligned_pruned_column_ids.begin(), aligned_pruned_column_ids.end(),
                            stored_table_node->pruned_column_ids().begin(),
                            stored_table_node->pruned_column_ids().end(),
                            std::back_inserter(updated_pruned_column_ids));
      updated_pruned_column_ids.shrink_to_fit();
      aligned_pruned_column_ids_for_node[stored_table_node] = updated_pruned_column_ids;
    } else {
      aligned_pruned_column_ids_for_node[stored_table_node] = stored_table_node->pruned_column_ids();
    }
  }
}

}  // namespace opossum
