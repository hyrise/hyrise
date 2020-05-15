#pragma once

#include <unordered_map>
#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

// Modified hash code generation for StoredTableNodes where column pruning information is omitted. Struct is used to
// enable hash based containers containing std::shared_ptr<StoredTableNode>.
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
// enable hash based containers containing std::shared_ptr<StoredTableNode>.
struct StoredTableNodeSharedPtrEqual final {
  size_t operator()(const std::shared_ptr<StoredTableNode>& lhs, const std::shared_ptr<StoredTableNode>& rhs) const {
    return lhs == rhs || (lhs->table_name == rhs->table_name && lhs->pruned_chunk_ids() == rhs->pruned_chunk_ids());
  }
};

template <typename Value>
using StoredTableNodeUnorderedMap =
  std::unordered_map<std::shared_ptr<StoredTableNode>, Value, StoredTableNodeSharedPtrHash,
  StoredTableNodeSharedPtrEqual>;

class StoredTableColumnAlignmentRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;
  void recursively_gather_stored_table_nodes(const std::shared_ptr<AbstractLQPNode>& node,
  	StoredTableNodeUnorderedMap<std::vector<std::shared_ptr<StoredTableNode>>>& gathered_stored_table_nodes_for_node,
  	StoredTableNodeUnorderedMap<std::vector<ColumnID>>& aligned_pruned_column_ids_for_node) const;
};

}  // namespace opossum
