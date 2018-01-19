#include <algorithm>
#include <iostream>

#include "index_evaluator.hpp"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"
#include "optimizer/column_statistics.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

IndexEvaluator::IndexEvaluator() {}

std::vector<IndexEvaluation> IndexEvaluator::evaluate_indices(const SystemStatistics& statistics) {
  _indices.clear();

  // ToDo(group01): _find_existing_indices();

  // Investigate query cache
  const auto& recent_queries = statistics.recent_queries();
  _access_recods.clear();
  for (const auto& cache_entry : recent_queries) {
    for (const auto& tree_root : cache_entry.query_plan.tree_roots()) {
      _inspect_operator(tree_root, cache_entry.access_frequency);
    }
  }
  _aggregate_access_records();

  _estimate_cost();

  return _calculate_desirability();
}

void IndexEvaluator::_inspect_operator(const std::shared_ptr<const AbstractOperator>& op, size_t query_frequency) {
  if (const auto& table_scan = std::dynamic_pointer_cast<const TableScan>(op)) {
    // skipped because it is skipped in lqp_translator
    // if (const auto& validate = std::dynamic_pointer_cast<const Validate>(table_scan->input_left())) {
    if (const auto& get_table = std::dynamic_pointer_cast<const GetTable>(table_scan->input_left())) {
      const auto& table_name = get_table->table_name();
      ColumnID column_id = table_scan->left_column_id();
      std::cout << "TableScan on table " << table_name << " and column " << column_id << "\n";

      auto table = StorageManager::get().get_table(table_name);
      auto table_statistics = table->table_statistics();
      // auto column_statistics = table->table_statistics()->column_statistics().at(column_id);
      auto compare_value = boost::get<AllTypeVariant>(table_scan->right_parameter());
      auto predicate_statistics =
          table_statistics->predicate_statistics(column_id, table_scan->scan_type(), table_scan->right_parameter());
      auto selectivity =
          table_statistics->row_count() > 0 ? predicate_statistics->row_count() / table_statistics->row_count() : 1.0f;
      std::cout << table_name << "." << table->column_name(column_id);
      std::cout << " " << *table_statistics << "\n";
      std::cout << "predicate statistics: ";
      std::cout << " " << *predicate_statistics << "\n";
      std::cout << "-> selectivity: " << predicate_statistics->row_count() / (table_statistics->row_count() + 1)
                << "\n";

      _access_recods.emplace_back(AccessRecord{table_name, column_id, query_frequency, selectivity});
    }
    //}
  } else {
    if (op->input_left()) {
      _inspect_operator(op->input_left(), query_frequency);
    }
    if (op->input_right()) {
      _inspect_operator(op->input_right(), query_frequency);
    }
  }
}

void IndexEvaluator::_aggregate_access_records() {
  for (const auto& access_record : _access_recods) {
    float saved_work = (1.0 / access_record.selectivity) * access_record.number_of_usages;

    IndexSpec index_spec = std::make_pair(access_record.table_name, access_record.column_id);
    _indices[index_spec].saved_work += saved_work;
  }
}

void IndexEvaluator::_estimate_cost() {
  // ToDo(group01): useful logic, e.g. number of chunks to estimate table size
  for (auto& index_data : _indices) {
    index_data.second.memory_cost = 1;
  }
}

std::vector<IndexEvaluation> IndexEvaluator::_calculate_desirability() {
  // ToDo(group01): use IndexEvaluation instead of IndexEvaluatorData
  //    unless we find more desirability components than only saved work
  std::vector<IndexEvaluation> evaluations;
  for (auto& index_data : _indices) {
    evaluations.emplace_back(index_data.first.first, index_data.first.second);
    evaluations.back().desirablility = index_data.second.saved_work;
    evaluations.back().memory_cost = index_data.second.memory_cost;
  }
  return evaluations;
}
}  // namespace opossum
