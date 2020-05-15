#include "optimizer/strategy/stored_table_column_alignment_rule.hpp"

#include <algorithm>
#include <memory>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace {
// Modified hash code generation for StoredTableNodes where column pruning information is omitted. Struct is used to
// enable hash-based containers containing std::shared_ptr<StoredTableNode>.
struct StoredTableNodeSharedPtrHash final {
  size_t operator()(const std::shared_ptr<opossum::StoredTableNode>& node) const {
    size_t hash{0};
    boost::hash_combine(hash, node->table_name);
    for (const auto& pruned_chunk_id : node->pruned_chunk_ids()) {
      boost::hash_combine(hash, static_cast<size_t>(pruned_chunk_id));
    }
    return hash;
  }
};

// Modified equals evaluation code for StoredTableNodes where column pruning information is omitted. Struct is used to
// enable hash-based containers containing std::shared_ptr<StoredTableNode>.
struct StoredTableNodeSharedPtrEqual final {
  size_t operator()(const std::shared_ptr<opossum::StoredTableNode>& lhs,
                    const std::shared_ptr<opossum::StoredTableNode>& rhs) const {
    return lhs == rhs || (lhs->table_name == rhs->table_name && lhs->pruned_chunk_ids() == rhs->pruned_chunk_ids());
  }
};

template <typename Value>
using ColumnPruningAgnosticStoredTableNodeMap =
    std::unordered_map<std::shared_ptr<opossum::StoredTableNode>, Value, StoredTableNodeSharedPtrHash,
                       StoredTableNodeSharedPtrEqual>;
}  // namespace

namespace opossum {

void StoredTableColumnAlignmentRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  ColumnPruningAgnosticStoredTableNodeMap<std::vector<std::shared_ptr<StoredTableNode>>>
      gathered_stored_table_nodes_for_node;
  ColumnPruningAgnosticStoredTableNodeMap<std::vector<ColumnID>> aligned_pruned_column_ids_for_node;

  visit_lqp(root, [&](const auto& node) {
    if (node->type == LQPNodeType::StoredTable) {
      const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);

      auto& gathered_stored_table_nodes = gathered_stored_table_nodes_for_node[stored_table_node];
      gathered_stored_table_nodes.emplace_back(stored_table_node);

      // update maximum superset of pruned column ids for a StoredTableNodes with the same table name and same set of
      // pruned chunk ids
      if (aligned_pruned_column_ids_for_node.contains(stored_table_node)) {
        auto& aligned_pruned_column_ids = aligned_pruned_column_ids_for_node[stored_table_node];
        std::vector<ColumnID> updated_pruned_column_ids{};
        updated_pruned_column_ids.reserve(aligned_pruned_column_ids.size() +
                                          stored_table_node->pruned_column_ids().size());
        // sortedness of input containers is guaranteed by set_pruned_column_ids
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
    return LQPVisitation::VisitInputs;
  });

  // iterate over groups of StoredTableNode and set the maximum superset of columns for each StoredTableNode in each
  // StoredTableNode group. A group is characterized by the table name and the pruned chunk ids.
  for (const auto& node_nodes_pair : gathered_stored_table_nodes_for_node) {
    for (const auto& stored_table_node : node_nodes_pair.second) {
      stored_table_node->set_pruned_column_ids(aligned_pruned_column_ids_for_node[stored_table_node]);
    }
  }
}

}  // namespace opossum
