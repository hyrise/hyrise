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

// Kind of arbitrarily chosen, but the following paper suggests something similar
// Access Path Selection in Main-Memory Optimized Data Systems: Should I Scan or Should I Probe?
constexpr float INDEX_SCAN_SELECTIVITY_THRESHOLD = 0.01f;

// From GroupKeyPaper
constexpr float INDEX_SCAN_ROW_COUNT_THRESHOLD = 1000.0f;

std::string IndexScanRule::name() const { return "Index Scan Rule"; }

bool IndexScanRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type() == LQPNodeType::Predicate) {
    const auto& child = node->left_child();

    if (child->type() == LQPNodeType::StoredTable) {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
      const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(child);
      const auto table = StorageManager::get().get_table(stored_table_node->table_name());

      const auto columns_of_indexes = table->get_columns_of_indexes();
      for (const auto& indexed_columns : columns_of_indexes) {
        if (_is_index_scan_applicable(indexed_columns, predicate_node)) {
          predicate_node->set_scan_type(ScanType::IndexScan);
        }
      }
    }
  }

  return _apply_to_children(node);
}

bool IndexScanRule::_is_index_scan_applicable(const std::vector<ColumnID>& indexed_columns,
                                              const std::shared_ptr<PredicateNode>& predicate_node) const {
  if (!_is_single_column_index(indexed_columns)) return false;

  // Currently, we do not support two column predicates
  if (is_column_id(predicate_node->value())) return false;

  ColumnID column_id = predicate_node->column_reference().original_column_id();
  if (indexed_columns[0] != column_id) return false;

  const auto row_count_table = predicate_node->left_child()->derive_statistics_from(nullptr, nullptr)->row_count();
  if (row_count_table < INDEX_SCAN_ROW_COUNT_THRESHOLD) return false;

  const auto row_count_predicate = predicate_node->derive_statistics_from(predicate_node->left_child())->row_count();
  const float selectivity = row_count_predicate / row_count_table;

  if (selectivity > INDEX_SCAN_SELECTIVITY_THRESHOLD) return false;

  return true;
}

inline bool IndexScanRule::_is_single_column_index(const std::vector<ColumnID>& indexed_columns) const {
  return indexed_columns.size() == 1;
}

}  // namespace opossum
