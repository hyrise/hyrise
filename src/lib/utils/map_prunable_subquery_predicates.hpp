#pragma once

#include "logical_query_plan/stored_table_node.hpp"
#include "operators/get_table.hpp"

namespace hyrise {

/**
 * We use predicates with uncorrelated subqueries for dynamic pruning (see get_table.hpp). These predicates are attached
 * to StoredTableNodes and GetTable operators. We must correctly maintain this information for LQP/PQP deep copies and
 * when translating LQPs to PQPs. We cannot do this while copying/translating the LQP/PQP. Though we keep a mapping from
 * source to target item (LQP node to LQP node for LQP copy, operator to operator for PQP copy, LQP node to operator for
 * translation), we cannot use it for prunable subquery predicates.
 * Our copy/translation happens recursively from root to leave (StoredTableNode/GetTable). Thus, the mapping does not
 * yet contain the copied/translated predicate that we need in the leaves. As a consequence, we assign prunable subquery
 * predicates after we copied/translated the entire query plan.
 */
template <typename Mapping>
void map_prunable_subquery_predicates(const Mapping& mapping) {
  using SourceType = typename Mapping::key_type;
  using TargetType = typename Mapping::mapped_type::element_type;
  using TargetPredicateType = std::conditional_t<std::is_same_v<TargetType, AbstractOperator>,
                                                 std::shared_ptr<const TargetType>, std::shared_ptr<TargetType>>;
  using SourceItemType =
      std::conditional_t<std::is_same_v<SourceType, const AbstractOperator*>, GetTable, StoredTableNode>;
  using TargetItemType = std::conditional_t<std::is_same_v<TargetType, AbstractOperator>, GetTable, StoredTableNode>;

  for (const auto& [item, mapped_item] : mapping) {
    // Skip if wrong operator/LQP node.
    if constexpr (std::is_same_v<SourceType, const AbstractOperator*>) {
      if (item->type() != OperatorType::GetTable) {
        continue;
      }
    } else {
      if (item->type == LQPNodeType::StoredTable) {
        continue;
      }
    }

    // Fetch prunable subquery predicates.
    const auto prunable_subquery_items = static_cast<const SourceItemType&>(*item).prunable_subquery_predicates();

    // Skip if the predicates are empty.
    if (prunable_subquery_items.empty()) {
      continue;
    }

    // Find mapped predicate for each prunable subquery predicate.
    auto mapped_prunable_subquery_items = std::vector<typename TargetPredicateType::weak_type>{};
    mapped_prunable_subquery_items.reserve(prunable_subquery_items.size());
    for (const auto& prunable_subquery_item : prunable_subquery_items) {
      auto prunable_item = SourceType{};
      if constexpr (std::is_same_v<SourceType, const AbstractOperator*>) {
        prunable_item = prunable_subquery_item.get();
      } else {
        prunable_item = prunable_subquery_item;
      }
      DebugAssert(mapping.contains(prunable_item), "Could not find referenced node. LQP/PQP is invalid.");
      mapped_prunable_subquery_items.emplace_back(mapping.at(prunable_item));
    }

    // Set mapped prunable subqery predicates for mapped item.
    static_cast<TargetItemType&>(*mapped_item).set_prunable_subquery_predicates(mapped_prunable_subquery_items);
  }
}

}  // namespace hyrise
