#include "optimizer/strategy/stored_table_column_alignment_rule.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace {
using namespace hyrise;  // NOLINT

// Modified hash code generation for StoredTableNodes where column pruning information is omitted. Struct is used to
// enable hash-based containers containing std::shared_ptr<StoredTableNode>.
struct StoredTableNodeSharedPtrHash final {
  size_t operator()(const std::shared_ptr<StoredTableNode>& node) const {
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
  size_t operator()(const std::shared_ptr<StoredTableNode>& lhs, const std::shared_ptr<StoredTableNode>& rhs) const {
    DebugAssert(std::is_sorted(lhs->pruned_chunk_ids().cbegin(), lhs->pruned_chunk_ids().cend()),
                "Expected sorted vector of ChunkIDs");
    DebugAssert(std::is_sorted(rhs->pruned_chunk_ids().cbegin(), rhs->pruned_chunk_ids().cend()),
                "Expected sorted vector of ChunkIDs");
    return lhs == rhs || (lhs->table_name == rhs->table_name && lhs->pruned_chunk_ids() == rhs->pruned_chunk_ids());
  }
};

using ColumnPruningAgnosticMultiSet =
    std::unordered_multiset<std::shared_ptr<StoredTableNode>, StoredTableNodeSharedPtrHash,
                            StoredTableNodeSharedPtrEqual>;

ColumnPruningAgnosticMultiSet collect_stored_table_nodes(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqps) {
  auto grouped_stored_table_nodes = ColumnPruningAgnosticMultiSet{};
  // Iterate over the given LQPs and store all StoredTableNodes in multiple sets/groups: Nodes of the same set/group
  // share the same table name and the same pruned ChunkIDs.
  for (const auto& lqp : lqps) {
    const auto nodes = lqp_find_nodes_by_type(lqp, LQPNodeType::StoredTable);
    std::for_each(nodes.begin(), nodes.end(), [&](const auto& node) {
      grouped_stored_table_nodes.insert(std::static_pointer_cast<StoredTableNode>(node));
    });
  }
  return grouped_stored_table_nodes;
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

namespace hyrise {

std::string StoredTableColumnAlignmentRule::name() const {
  static const auto name = std::string{"StoredTableColumnAlignmentRule"};
  return name;
}

/**
 * The default implementation of this function optimizes a given LQP and all of its subquery LQPs individually.
 * However, as we do not want to align StoredTableNodes per plan but across all plans, we override it accordingly.
 */
void StoredTableColumnAlignmentRule::apply_to_plan(const std::shared_ptr<LogicalPlanRootNode>& root_node) const {
  // (1) Collect all plans
  auto lqps = std::vector<std::shared_ptr<AbstractLQPNode>>();
  lqps.emplace_back(std::static_pointer_cast<AbstractLQPNode>(root_node));
  const auto subquery_expressions_by_lqp = collect_lqp_subquery_expressions_by_lqp(root_node);
  for (const auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
    lqps.emplace_back(lqp);
  }

  // (2) Collect all StoredTableNodes and group them by their key (same table name and same set of pruned chunks).
  const auto grouped_stored_table_nodes = collect_stored_table_nodes(lqps);

  // (3) Align grouped StoredTableNodes
  align_pruned_column_ids(grouped_stored_table_nodes);
}

void StoredTableColumnAlignmentRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  Fail("Did not expect this function to be called.");
}

}  // namespace hyrise
