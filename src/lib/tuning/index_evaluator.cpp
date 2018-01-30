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

std::vector<IndexEvaluation> IndexEvaluator::evaluate_indices(const SystemStatistics& statistics) {
  // Scan recent queries for indexable table column accesses
  // sets _new_indices and _saved_work
  _find_useful_indices(statistics.cache());

  _indices.clear();
  // Scan table columns for existing indices
  _add_existing_indices();
  _add_new_indices();

  _calculate_memory_cost();
  _calculate_desirability();

  return std::vector<IndexEvaluation>(_indices);
}

/**
 * Scans the supplied query cache for predicate nodes that might profit from an index.
 * The resulting column references are stored in _new_indices and
 * _saved_work gives an estimate of how much work would have been saved if
 * the cached queries would have been executed using an index on the column.
 */
void IndexEvaluator::_find_useful_indices(const SQLQueryCache<std::shared_ptr<SQLQueryPlan>>& cache) {
  // <QueryCache> -> _operations
  _inspect_query_cache(cache);

  _access_records.clear();
  for (const auto& operation : _operations) {
    // -> _access_records
    _inspect_operator(operation.operator_tree, operation.frequency);
  }

  // _access_records -> _new_indices, _saved_work
  _aggregate_access_records();
}

/**
 * Scans all tables and columns in the system for existing indices.
 * The results are saved as IndexEvaluations in _indices.
 * Also the columns with indices on them are removed from _new_indices.
 *
 * This iterates over all tables, gets the first chunk each and checks whether
 * there are indices for each column.
 * This means that this method only checks for single-column indices (but will also
 * find multi-column indices suitable for that single column).
 * It is assumed that an index for a column is present either in all chunks or none.
 */
void IndexEvaluator::_add_existing_indices() {
  for (const auto& table_name : StorageManager::get().table_names()) {
    const auto& table = StorageManager::get().get_table(table_name);
    const auto& first_chunk = table->get_chunk(ChunkID{0});

    for (const auto& column_name : table->column_names()) {
      const auto& column_id = table->column_id_by_name(column_name);
      auto column_ids = std::vector<ColumnID>();
      column_ids.emplace_back(column_id);
      auto indices = first_chunk->get_indices(column_ids);
      for (const auto& index : indices) {
        _indices.emplace_back(table_name, column_id, true);
        _indices.back().type = index->type();
        _new_indices.erase({table_name, column_id});
      }
      if (indices.size() > 1) {
        LOG_DEBUG("Found " << indices.size() << " indices on " << table_name << "." << column_name);
      } else if (indices.size() > 0) {
        LOG_DEBUG("Found index on " << table_name << "." << column_name);
      }
    }
  }
}

/**
 * Adds the remaining entries in _new_indices as IndexEvaluations to _indices.
 *
 * The new index proposals type is determinded by _propose_index_type()
 */
void IndexEvaluator::_add_new_indices() {
  for (const auto& index_spec : _new_indices) {
    _indices.emplace_back(index_spec.table_name, index_spec.column_id, false);
    _indices.back().type = _propose_index_type(_indices.back());
  }
}

/**
 * Assigns the memory_cost member of all index proposals.
 * The information is queried it from the index implementation if it exists
 * and otherwise predicted.
 */
void IndexEvaluator::_calculate_memory_cost() {
  for (auto& index_evaluation : _indices) {
    if (index_evaluation.exists) {
      index_evaluation.memory_cost = _report_memory_cost(index_evaluation);
    } else {
      index_evaluation.memory_cost = _predict_memory_cost(index_evaluation);
    }
  }
}

/**
 * Assigns the desirability member of all index proposals.
 * Currently only _saved_work entries are considered.
 */
void IndexEvaluator::_calculate_desirability() {
  // ToDo(group01) also consider additional work caused by this index
  // = table insert rate * index update cost ?
  for (auto& index_evaluation : _indices) {
    ColumnRef index_spec{index_evaluation.table_name, index_evaluation.column_id};
    if (_saved_work.count(index_spec) > 0) {
      index_evaluation.desirablility = _saved_work[index_spec];
    } else {
      index_evaluation.desirablility = 0.0f;
    }
  }
}

void IndexEvaluator::_inspect_query_cache(const SQLQueryCache<std::shared_ptr<SQLQueryPlan>>& cache) {
  _operations.clear();

  // ToDo(group01) introduce values() method in AbstractCache interface and implement in all subclasses
  //   const auto& query_plan_cache = SQLQueryOperator::get_query_plan_cache().cache();
  // ToDo(group01) implement for cache implementations other than GDFS cache
  auto gdfs_cache_ptr = dynamic_cast<const GDFSCache<std::string, std::shared_ptr<SQLQueryPlan>>*>(&cache.cache());
  Assert(gdfs_cache_ptr, "Expected GDFS Cache");

  const boost::heap::fibonacci_heap<GDFSCache<std::string, std::shared_ptr<SQLQueryPlan>>::GDFSCacheEntry>&
      fibonacci_heap = gdfs_cache_ptr->queue();

  LOG_DEBUG("Query plan cache (size: " << fibonacci_heap.size() << "):");
  auto cache_iterator = fibonacci_heap.ordered_begin();
  auto cache_end = fibonacci_heap.ordered_end();

  for (; cache_iterator != cache_end; ++cache_iterator) {
    const GDFSCache<std::string, std::shared_ptr<SQLQueryPlan>>::GDFSCacheEntry& entry = *cache_iterator;
    LOG_DEBUG("  -> Query '" << entry.key << "' frequency: " << entry.frequency << " priority: " << entry.priority);
    for (const auto& operator_tree : entry.value->tree_roots()) {
      _operations.emplace_back(operator_tree, entry.frequency);
    }
  }
}

void IndexEvaluator::_inspect_operator(const std::shared_ptr<const AbstractOperator>& op, size_t query_frequency) {
  if (const auto& table_scan = std::dynamic_pointer_cast<const TableScan>(op)) {
    // skipped because it is skipped in lqp_translator
     if (const auto& validate = std::dynamic_pointer_cast<const Validate>(table_scan->input_left())) {
    if (const auto& get_table = std::dynamic_pointer_cast<const GetTable>(validate->input_left())) {
      const auto& table_name = get_table->table_name();
      ColumnID column_id = table_scan->left_column_id();

      auto table = StorageManager::get().get_table(table_name);
      auto table_statistics = table->table_statistics();
      // auto column_statistics = table->table_statistics()->column_statistics().at(column_id);
      auto compare_value = boost::get<AllTypeVariant>(table_scan->right_parameter());
      auto predicate_statistics = table_statistics->predicate_statistics(column_id, table_scan->predicate_condition(),
                                                                         table_scan->right_parameter());
      auto selectivity = table_statistics->row_count() > 0
                             ? predicate_statistics->row_count() / table_statistics->row_count()
                             : 1.0f;

      const auto& column_name = table->column_name(column_id);
      LOG_DEBUG("Found TableScan on table " << table_name << " and column " << column_name << " (selectivity: " << selectivity << ")");

        /*
      std::cout << table_name << "." << table->column_name(column_id);
      std::cout << " " << *table_statistics << "\n";
      std::cout << "predicate statistics: ";
      std::cout << " " << *predicate_statistics << "\n";
      std::cout << "-> selectivity: " << predicate_statistics->row_count() / (table_statistics->row_count() + 1)
                << "\n";
         */

      _access_records.emplace_back(AccessRecord{table_name, column_id, query_frequency, selectivity});
      }
    }
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
  _saved_work.clear();
  _new_indices.clear();
  for (const auto& access_record : _access_records) {
    float saved_work = (1.0 / access_record.selectivity) * access_record.number_of_usages;

    ColumnRef index_spec{access_record.table_name, access_record.column_id};
    if (_saved_work.count(index_spec) > 0) {
      _saved_work[index_spec] += saved_work;
    } else {
      _saved_work[index_spec] = saved_work;
      _new_indices.insert(index_spec);
    }
  }
}

ColumnIndexType IndexEvaluator::_propose_index_type(const IndexEvaluation& index_evaluation) const {
  return ColumnIndexType::GroupKey;
}

float IndexEvaluator::_predict_memory_cost(const IndexEvaluation& index_evaluation) const {
  auto table = StorageManager::get().get_table(index_evaluation.table_name);
  auto table_statistics = table->table_statistics();
  auto predicate_statistics =
      table_statistics->predicate_statistics(index_evaluation.column_id, PredicateCondition::IsNotNull, 0);
  // ToDo(group01) find a better way to determine a columns value count! (obtain ColumnStatistics directly?)
  auto value_count = predicate_statistics->column_statistics().at(index_evaluation.column_id)->distinct_count();

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

float IndexEvaluator::_report_memory_cost(const IndexEvaluation& index_evaluation) const {
  auto table = StorageManager::get().get_table(index_evaluation.table_name);
  float memory_cost = 0.0f;
  for (ChunkID chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    auto index = chunk->get_index(index_evaluation.type, std::vector<ColumnID>{index_evaluation.column_id});
    if (index) {
      memory_cost += index->memory_consumption();
    }
  }
  return memory_cost;
}

}  // namespace opossum
