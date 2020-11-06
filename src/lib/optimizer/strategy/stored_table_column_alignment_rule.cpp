#include "optimizer/strategy/stored_table_column_alignment_rule.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace {
using namespace opossum;  // NOLINT

// Modified hash code generation for StoredTableNodes where column pruning information is omitted. Struct is used to
// enable hash-based containers containing std::shared_ptr<StoredTableNode>.
struct StoredTableNodeSharedPtrHash final {
  size_t operator()(const std::shared_ptr<opossum::StoredTableNode>& node) const {
    DebugAssert(std::is_sorted(node->pruned_chunk_ids().cbegin(), node->pruned_chunk_ids().cend()),
                "Expected sorted vector of ChunkIDs");
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
    DebugAssert(std::is_sorted(lhs->pruned_chunk_ids().cbegin(), lhs->pruned_chunk_ids().cend()),
                "Expected sorted vector of ChunkIDs");
    DebugAssert(std::is_sorted(rhs->pruned_chunk_ids().cbegin(), rhs->pruned_chunk_ids().cend()),
                "Expected sorted vector of ChunkIDs");
    return lhs == rhs || (lhs->table_name == rhs->table_name && lhs->pruned_chunk_ids() == rhs->pruned_chunk_ids());
  }
};

using ColumnPruningAgnosticMultiSet =
    std::unordered_multiset<std::shared_ptr<opossum::StoredTableNode>, StoredTableNodeSharedPtrHash,
                            StoredTableNodeSharedPtrEqual>;

void collect_stored_table_nodes(ColumnPruningAgnosticMultiSet& grouped_stored_table_nodes,
                                const std::shared_ptr<AbstractLQPNode>& root) {
  // Iterate over the LQP and store all StoredTableNodes in multiple sets/groups: nodes of the same set/group have the
  // same table name and the same pruned chunks.
  visit_lqp(root, [&](const auto& node) {
    if (node->type == LQPNodeType::StoredTable) {
      const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
      DebugAssert(stored_table_node, "LQPNode with type 'StoredTable' could not be casted to a StoredTableNode.");
      grouped_stored_table_nodes.emplace(stored_table_node);
    }
    return LQPVisitation::VisitInputs;
  });
}

void align_pruned_column_ids(const ColumnPruningAgnosticMultiSet& grouped_stored_table_nodes) {
  /**
   * For each group of StoredTableNodes,
   * (1) iterate over the nodes and calculate the set intersection of pruned column ids and
   * (2) iterate over the nodes and set the aligned pruned column ids.
   */
  for (auto group_representative = grouped_stored_table_nodes.begin();
       group_representative != grouped_stored_table_nodes.end(); ++group_representative) {
    std::optional<std::vector<ColumnID>> aligned_pruned_column_ids;
    const auto& group_range = grouped_stored_table_nodes.equal_range(*group_representative);
    for (auto group_iter = group_range.first; group_iter != group_range.second; ++group_iter) {
      const auto& stored_table_node = *group_iter;
      if (!aligned_pruned_column_ids) {
        aligned_pruned_column_ids = stored_table_node->pruned_column_ids();
      } else {
        std::vector<ColumnID> updated_pruned_column_ids{};
        DebugAssert(std::is_sorted(aligned_pruned_column_ids->cbegin(), aligned_pruned_column_ids->cend()),
                    "Expected sorted vector of ColumnIDs");
        DebugAssert(std::is_sorted(stored_table_node->pruned_column_ids().cbegin(),
                                   stored_table_node->pruned_column_ids().cend()),
                    "Expected sorted vector of ColumnIDs");
        std::set_intersection(aligned_pruned_column_ids->cbegin(), aligned_pruned_column_ids->cend(),
                              stored_table_node->pruned_column_ids().cbegin(),
                              stored_table_node->pruned_column_ids().cend(),
                              std::back_inserter(updated_pruned_column_ids));
        aligned_pruned_column_ids = std::move(updated_pruned_column_ids);
      }
    }
    for (auto group_iter = group_range.first; group_iter != group_range.second; ++group_iter) {
      (*group_iter)->set_pruned_column_ids(*aligned_pruned_column_ids);
    }
  }
}

}  // namespace

namespace opossum {

/**
 * This rule optimizes root and subquery LQPs all at once to be more effective. Therefore, we have to override the
 * default implementation of AbstractRule::apply, which optimizes root and subquery LQPs individually, one-by-one.
 */
void StoredTableColumnAlignmentRule::apply(const std::shared_ptr<AbstractLQPNode>& root_node) const {
  _apply_to(root_node);
}

void StoredTableColumnAlignmentRule::_apply_to(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // (1) Collect all distinct LQPs:   a) Root node
  //                                  b) Subquery LQPs
  auto lqps = std::vector<std::shared_ptr<AbstractLQPNode>>{lqp_root};
  auto subquery_expressions_by_lqp = SubqueryExpressionsByLQP{};
  collect_subquery_expressions_by_lqp(subquery_expressions_by_lqp, lqp_root);
  for (const auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
    lqps.emplace_back(lqp);
  }

  // (2) Collect all StoredTableNodes and group them by their key (same table name and same set of pruned chunks).
  auto grouped_stored_table_nodes = ColumnPruningAgnosticMultiSet{};
  for (const auto& lqp_node : lqps) {
    collect_stored_table_nodes(grouped_stored_table_nodes, lqp_node);
  }

  // (3) Align grouped StoredTableNodes
  align_pruned_column_ids(grouped_stored_table_nodes);
}

}  // namespace opossum
