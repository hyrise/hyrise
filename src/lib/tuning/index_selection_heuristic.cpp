#include <algorithm>

#include "index_selection_heuristic.hpp"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"

namespace opossum {

IndexSelectionHeuristic::IndexSelectionHeuristic() {}

const std::vector<IndexProposal>& IndexSelectionHeuristic::recommend_changes(const SystemStatistics& statistics) {
  _index_proposals.clear();

  // Investigate query cache
  const auto& recent_queries = statistics.recent_queries();

  for (const auto& query_plan : recent_queries) {
    for (const auto& tree_root : query_plan.tree_roots()) {
      _inspect_operator(tree_root);
    }
  }

  // Sort by desirability, highest value first
  std::sort(_index_proposals.begin(), _index_proposals.end(), std::greater<IndexProposal>());

  return _index_proposals;
}

void IndexSelectionHeuristic::_inspect_operator(const std::shared_ptr<const AbstractOperator>& op) {
  if (const auto& table_scan = std::dynamic_pointer_cast<const TableScan>(op)) {
    if (const auto& validate = std::dynamic_pointer_cast<const Validate>(table_scan->input_left())) {
      if (const auto& get_table = std::dynamic_pointer_cast<const GetTable>(validate->input_left())) {
        const auto& table_name = get_table->table_name();
        ColumnID column_id = table_scan->left_column_id();
        _index_proposals.emplace_back(IndexProposal{table_name, column_id, 1.0});
        std::cout << "TableScan on table " << table_name << " and column " << column_id << "\n";
      }
    }
  } else {
    if (op->input_left()) {
      _inspect_operator(op->input_left());
    }
    if (op->input_right()) {
      _inspect_operator(op->input_right());
    }
  }
}

}  // namespace opossum
