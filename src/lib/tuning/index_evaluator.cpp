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
  _access_recods.clear();
  _index_evaluations.clear();

  // Investigate query cache
  const auto& recent_queries = statistics.recent_queries();

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
    // ToDo(group01): Use more efficient data structure to find existing evaluations
    bool match = false;
    for (auto& index_evaluation : _index_evaluations) {
      if (index_evaluation.table_name == access_record.table_name &&
          index_evaluation.column_id == access_record.column_id) {
        index_evaluation.saved_work += (1.0 / access_record.selectivity) * access_record.number_of_usages;
        match = true;
        break;
      }
    }
    if (!match) {
      // create new evaluation as no existing one fits this record
      _index_evaluations.emplace_back(access_record.table_name, access_record.column_id);
      _index_evaluations.back().saved_work += (1.0 / access_record.selectivity) * access_record.number_of_usages;
    }
  }
}

void IndexEvaluator::_estimate_cost() {
  // ToDo(group01): useful logic, e.g. number of chunks to estimate table size
  for (auto& evaluation : _index_evaluations) {
    evaluation.cost = 1;
  }
}

std::vector<IndexEvaluation> IndexEvaluator::_calculate_desirability() {
  // Map absolute usage + cost values to relative values (across all index proposals)
  // ToDo(group01: if we plan to continue with this approach, extract a calculateRelativeValues(accessor) method
  //               to deduplicate code.

  auto max_num_usages_element = std::max_element(_index_evaluations.begin(), _index_evaluations.end(),
                                                 IndexEvaluatorData::compare_number_of_usages);
  auto max_num_usages = static_cast<float>(max_num_usages_element->number_of_usages);

  auto max_cost_element =
      std::max_element(_index_evaluations.begin(), _index_evaluations.end(), IndexEvaluatorData::compare_cost);
  auto min_cost_element =
      std::min_element(_index_evaluations.begin(), _index_evaluations.end(), IndexEvaluatorData::compare_cost);
  auto max_cost = static_cast<float>(max_cost_element->cost);
  auto min_cost = static_cast<float>(min_cost_element->cost);
  // If there is only one cost value, add one to prevent a division by zero error
  if (max_cost == min_cost) {
    max_cost += 1.0f;
  }

  std::vector<IndexEvaluation> proposals;
  for (auto& evaluation : _index_evaluations) {
    float relative_num_usages = static_cast<float>(evaluation.number_of_usages) / max_num_usages;

    // From the cost minimum to cost maximum range, calculate where this cost value sits relatively
    float relative_cost = static_cast<float>(evaluation.cost - min_cost) / (max_cost - min_cost);

    // ToDo(group01): better "mixdown" logic
    // Since higher cost is bad, invert that value
    float desirability = 0.5f * relative_num_usages + 0.5f * (1.0f - relative_cost);
    proposals.emplace_back(evaluation.table_name, evaluation.column_id);
    proposals.back().desirablility = desirability;
  }
  return proposals;
}
}  // namespace opossum
