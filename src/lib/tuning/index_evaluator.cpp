#include <algorithm>
#include <iostream>

#include "index_evaluator.hpp"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"
#include "optimizer/column_statistics.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/index/base_index.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "utils/logging.hpp"

namespace opossum {

IndexEvaluator::IndexEvaluator() {}

void IndexEvaluator::_setup() { _saved_work.clear(); }

void IndexEvaluator::_process_access_record(const BaseIndexEvaluator::AccessRecord& record) {
  auto table_statistics = StorageManager::get().get_table(record.column_ref.table_name)->table_statistics();
  auto predicate_statistics =
      table_statistics->predicate_statistics(record.column_ref.column_id, record.condition, record.compare_value);
  auto total_rows = table_statistics->row_count();
  auto match_rows = predicate_statistics->row_count();
  auto unscanned_rows = total_rows - match_rows;
  float saved_work = unscanned_rows * record.query_frequency;
  if (_saved_work.count(record.column_ref) > 0) {
    _saved_work[record.column_ref] += saved_work;
  } else {
    _saved_work[record.column_ref] = saved_work;
  }
}

ColumnIndexType IndexEvaluator::_propose_index_type(const IndexEvaluation& index_evaluation) const {
  return ColumnIndexType::GroupKey;
}

float IndexEvaluator::_predict_memory_cost(const IndexEvaluation& index_evaluation) const {
  auto table = StorageManager::get().get_table(index_evaluation.column.table_name);
  auto table_statistics = table->table_statistics();
  auto predicate_statistics =
      table_statistics->predicate_statistics(index_evaluation.column.column_id, PredicateCondition::IsNotNull, 0);
  // ToDo(group01) find a better way to determine a columns value count! (obtain ColumnStatistics directly?)
  auto value_count = predicate_statistics->column_statistics().at(index_evaluation.column.column_id)->distinct_count();

  // ToDo(group01) understand DataType and sample column for average byte counts
  // DataType column_type = table->column_type(index_evaluation.column_id);
  auto value_bytes = 8;

  auto row_count = table->row_count();
  auto chunk_count = table->chunk_count();
  auto chunk_rows = row_count / chunk_count;
  auto chunk_values = value_count / chunk_count;

  float memory_cost_per_chunk =
      BaseIndex::predict_memory_consumption(index_evaluation.type, chunk_rows, chunk_values, value_bytes);
  return memory_cost_per_chunk * chunk_count;
}

float IndexEvaluator::_calculate_desirability(const IndexEvaluation& index_evaluation) const {
  if (_saved_work.count(index_evaluation.column) > 0) {
    return _saved_work.at(index_evaluation.column);
  } else {
    return 0.0f;
  }
}

}  // namespace opossum
