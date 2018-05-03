#include "index_tuning_evaluator.hpp"

#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/index/base_index.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

IndexTuningEvaluator::IndexTuningEvaluator() {}

void IndexTuningEvaluator::_setup() { _saved_work_per_index.clear(); }

void IndexTuningEvaluator::_process_access_record(const AbstractIndexTuningEvaluator::AccessRecord& record) {
  const auto table_statistics = StorageManager::get().get_table(record.column_ref.table_name)->table_statistics();
  // ToDo(anyone) adapt for multi column indices...
  const auto predicate_statistics =
      table_statistics->predicate_statistics(record.column_ref.column_ids[0], record.condition, record.compare_value);
  const auto total_rows = table_statistics->row_count();
  const auto match_rows = predicate_statistics->row_count();
  const auto unscanned_rows = total_rows - match_rows;
  const float saved_work = unscanned_rows * record.query_frequency;
  if (_saved_work_per_index.count(record.column_ref) > 0) {
    _saved_work_per_index[record.column_ref] += saved_work;
  } else {
    _saved_work_per_index[record.column_ref] = saved_work;
  }
}

ColumnIndexType IndexTuningEvaluator::_propose_index_type(const IndexTuningChoice& index_evaluation) const {
  return ColumnIndexType::GroupKey;
}

uintptr_t IndexTuningEvaluator::_predict_memory_cost(const IndexTuningChoice& index_evaluation) const {
  const auto table = StorageManager::get().get_table(index_evaluation.column_ref.table_name);
  // ToDo(anyone) adapt for multi column indices...
  const auto column_statistics =
      table->table_statistics()->column_statistics().at(index_evaluation.column_ref.column_ids[0]);
  const auto distinct_value_count = column_statistics->distinct_count();

  // Sum up column data type widths
  size_t value_bytes = 0;
  for (const auto column_id : index_evaluation.column_ref.column_ids) {
    const auto data_type = table->column_data_type(column_id);
    resolve_data_type(data_type, [&](auto boost_type) {
      using ColumnDataType = typename decltype(boost_type)::type;
      // This assumes that elements are self-contained
      value_bytes += sizeof(ColumnDataType);
    });
  }

  const auto row_count = table->row_count();
  const auto chunk_count = table->chunk_count();
  const auto average_rows_per_chunk = row_count / chunk_count;
  const auto chunk_distinct_values = distinct_value_count / chunk_count;

  const uintptr_t memory_cost_per_chunk = BaseIndex::estimate_memory_consumption(
      index_evaluation.type, average_rows_per_chunk, chunk_distinct_values, value_bytes);
  return memory_cost_per_chunk * chunk_count;
}

float IndexTuningEvaluator::_get_saved_work(const IndexTuningChoice& index_evaluation) const {
  if (_saved_work_per_index.count(index_evaluation.column_ref) > 0) {
    return _saved_work_per_index.at(index_evaluation.column_ref);
  } else {
    return 0.0f;
  }
}

}  // namespace opossum
