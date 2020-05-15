#include "optimizer/strategy/stored_table_column_alignment_rule.hpp"

#include <algorithm>
#include <memory>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

void StoredTableColumnAlignmentRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  std::cout << "APPLY TO\n";
  StoredTableNodeUnorderedMap<std::vector<std::shared_ptr<StoredTableNode>>> gathered_stored_table_nodes_for_node;
  StoredTableNodeUnorderedMap<std::vector<ColumnID>> aligned_pruned_column_ids_for_node;
  recursively_gather_stored_table_nodes(root, gathered_stored_table_nodes_for_node,
  	aligned_pruned_column_ids_for_node);
  
  for (const auto& kv : gathered_stored_table_nodes_for_node) {
  	std::cout << "StoredTableNode: " << kv.first->table_name << "\n";
  	for (const auto& chunk_id : kv.first->pruned_chunk_ids()) {
  		std::cout << "  " << chunk_id << "\n";
  	}
  	for (const auto& stored_table_node : kv.second) {
  		std::cout << "pruned columns: " << "\n";
  		for (const auto& column_id : stored_table_node->pruned_column_ids()) {
  			std::cout << column_id << " ";
  		}
  		std::cout << "\n";
  	}
  }

  // iterate over groups of StoredTableNode and set the minimum superset of columns for each StoredTableNode in each
  // group
  for (const auto& node_nodes_pair : gathered_stored_table_nodes_for_node) {
 	for (const auto& stored_table_node : node_nodes_pair.second) {
  		stored_table_node->set_pruned_column_ids(aligned_pruned_column_ids_for_node[stored_table_node]);
  	}
  }

  std::cout << "after update" << "\n";
  for (const auto& kv : gathered_stored_table_nodes_for_node) {
  	std::cout << "StoredTableNode: " << kv.first->table_name << "\n";
  	for (const auto& chunk_id : kv.first->pruned_chunk_ids()) {
  		std::cout << "  " << chunk_id << "\n";
  	}
  	for (const auto& stored_table_node : kv.second) {
  		std::cout << "pruned columns: " << "\n";
  		for (const auto& column_id : stored_table_node->pruned_column_ids()) {
  			std::cout << column_id << " ";
  		}
  		std::cout << "\n";
  	}
  }
}

void StoredTableColumnAlignmentRule::recursively_gather_stored_table_nodes(const std::shared_ptr<AbstractLQPNode>& node,
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

  	// update minimum superset of pruned column ids for a StoredTableNodes with the same table name and same set of
  	// pruned chunk ids
  	auto& aligned_pruned_column_ids = aligned_pruned_column_ids_for_node[stored_table_node];
  	std::vector<ColumnID> updated_pruned_column_ids{};
  	updated_pruned_column_ids.reserve(aligned_pruned_column_ids.size() +
  		stored_table_node->pruned_column_ids().size());
  	std::set_union(aligned_pruned_column_ids.begin(), aligned_pruned_column_ids.end(),
  		stored_table_node->pruned_column_ids().begin(), stored_table_node->pruned_column_ids().end(),
  		std::back_inserter(updated_pruned_column_ids));
  	updated_pruned_column_ids.shrink_to_fit();
  	aligned_pruned_column_ids_for_node[stored_table_node] = updated_pruned_column_ids;
  }
}

}  // namespace opossum
