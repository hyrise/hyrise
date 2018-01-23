#include "index_scan_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_parameter_variant.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string IndexScanRule::name() const { return "Index Scan Rule"; }

bool IndexScanRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type() == LQPNodeType::Predicate) {
  auto child = node->left_child();

    if (child->type() == LQPNodeType::StoredTable) {
      auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
      auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(child);
      auto table = StorageManager::get().get_table(stored_table_node->table_name());

      auto columns_of_indexes = table->get_columns_of_indexes();
      for (auto& indexed_columns: columns_of_indexes) {
        if (_is_index_scan_applicable(indexed_columns, predicate_node)) {
          predicate_node->set_scan_typee(ScanTypee::IndexScan);
        }
      }
    }
  }// else {
  return _apply_to_children(node);
  // }

  // return true;
}

bool IndexScanRule::_is_index_scan_applicable(const std::vector<ColumnID>& indexed_columns, const std::shared_ptr<PredicateNode>& predicate_node) const {
  if (!_is_single_column_index(indexed_columns)) return false;

  // Currently, we do not support two column predicates
  if (is_column_id(predicate_node->value())) return false;

  ColumnID column_id = predicate_node->column_reference().original_column_id();
  if (indexed_columns[0] != column_id) return false;

  auto row_count_table = predicate_node->left_child()->derive_statistics_from(nullptr, nullptr)->row_count();
  auto row_count_predicate = predicate_node->derive_statistics_from(predicate_node->left_child())->row_count();
  float selectivity = row_count_predicate / row_count_table;

  if (selectivity > 0.01f) return false;

  return true;
}

inline bool IndexScanRule::_is_single_column_index(const std::vector<ColumnID>& indexed_columns) const {
  return indexed_columns.size() == 1;
}
// bool IndexScanRule::_reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) const {
//   // Store original child and parent
//   auto child = predicates.back()->left_child();
//   const auto parents = predicates.front()->parents();
//   const auto child_sides = predicates.front()->get_child_sides();

//   const auto sort_predicate = [&](auto& left, auto& right) {
//     return left->derive_statistics_from(child)->row_count() > right->derive_statistics_from(child)->row_count();
//   };

//   if (std::is_sorted(predicates.begin(), predicates.end(), sort_predicate)) {
//     return false;
//   }

//   // Untie predicates from LQP, so we can freely retie them
//   for (auto& predicate : predicates) {
//     predicate->remove_from_tree();
//   }

//   // Sort in descending order
//   std::sort(predicates.begin(), predicates.end(), sort_predicate);

//   // Ensure that nodes are chained correctly
//   predicates.back()->set_left_child(child);

//   for (size_t parent_idx = 0; parent_idx < parents.size(); ++parent_idx) {
//     parents[parent_idx]->set_child(child_sides[parent_idx], predicates.front());
//   }

//   for (size_t predicate_index = 0; predicate_index < predicates.size() - 1; predicate_index++) {
//     predicates[predicate_index]->set_left_child(predicates[predicate_index + 1]);
//   }

//   return true;
// }

}  // namespace opossum
