#include "optimizer/strategy/stored_table_column_alignment_rule.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

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

using ColumnPruningAgnosticSet = std::unordered_set<std::shared_ptr<opossum::StoredTableNode>,
                                                    StoredTableNodeSharedPtrHash, StoredTableNodeSharedPtrEqual>;

using ColumnPruningAgnosticMultiSet =
    std::unordered_multiset<std::shared_ptr<opossum::StoredTableNode>, StoredTableNodeSharedPtrHash,
                            StoredTableNodeSharedPtrEqual>;
}  // namespace

namespace opossum {

void StoredTableColumnAlignmentRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  // Stores exactly one representative for each StoredTableNode group. Members of the same StoredTableNode group
  // have the same table name and pruned chunks.
  auto group_representatives = ColumnPruningAgnosticSet{};

  // Stores all StoredTableNodes grouped by key.
  auto grouped_stored_table_nodes = ColumnPruningAgnosticMultiSet{};

  // Iterate over the LQP and store all StoredTableNodes in multiple sets/groups: nodes of the same set/group have the
  // same table name and the same pruned chunks.
  visit_lqp(root, [&](const auto& node) {
    if (node->type == LQPNodeType::StoredTable) {
      const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
      DebugAssert(stored_table_node, "LQPNode with type 'StoredTable' could not be casted to a StoredTableNode.");
      group_representatives.emplace(stored_table_node);  // No modification if representative is already present
      grouped_stored_table_nodes.emplace(stored_table_node);
    }
    return LQPVisitation::VisitInputs;
  });

  // For each group of StoredTableNodes, (1) iterate over the nodes and calculate the set intersection of pruned
  // column ids and (2) iterate over the nodes and set the aligned pruned column ids.
  for (const auto& group_representative : group_representatives) {
    std::optional<std::vector<ColumnID>> aligned_pruned_column_ids;
    const auto& group_range = grouped_stored_table_nodes.equal_range(group_representative);
    for (auto group_iter = group_range.first; group_iter != group_range.second; ++group_iter) {
      const auto& stored_table_node = *group_iter;
      if (!aligned_pruned_column_ids) {
        aligned_pruned_column_ids = stored_table_node->pruned_column_ids();
      } else {
        std::vector<ColumnID> updated_pruned_column_ids{};
        std::set_intersection(aligned_pruned_column_ids->begin(), aligned_pruned_column_ids->end(),
                              stored_table_node->pruned_column_ids().begin(),
                              stored_table_node->pruned_column_ids().end(),
                              std::back_inserter(updated_pruned_column_ids));
        aligned_pruned_column_ids = std::move(updated_pruned_column_ids);
      }
    }
    for (auto group_iter = group_range.first; group_iter != group_range.second; ++group_iter) {
      (*group_iter)->set_pruned_column_ids(*aligned_pruned_column_ids);
    }
  }
}

}  // namespace opossum
