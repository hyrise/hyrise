#include "abstract_index_tuning_evaluator.hpp"

#include <algorithm>
#include <list>
#include <memory>
#include <string>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "sql/gdfs_cache.hpp"
#include "sql/sql_query_cache.hpp"
#include "storage/index/base_index.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

AbstractIndexTuningEvaluator::AbstractIndexTuningEvaluator() {}

void AbstractIndexTuningEvaluator::evaluate(std::vector<std::shared_ptr<TuningOption>>& choices) {
  // Allow concrete implementation to initialize
  _setup();

  // Scan query cache for indexable table column accesses and generate access records
  _access_records = _inspect_query_cache_and_generate_access_records();

  // Aggregate column accesses to set of new columns to index
  _new_indexes = _aggregate_access_records(_access_records);

  // Fill vector of evaluated choices
  _choices.clear();
  _add_choices_for_existing_indexes(_choices, _new_indexes);
  _add_choices_for_new_indexes(_choices, _new_indexes);

  // Evaluate
  for (auto& index_choice : _choices) {
    if (index_choice.index_exists) {
      index_choice.memory_cost = static_cast<float>(_existing_memory_cost(index_choice));
    } else {
      index_choice.type = _propose_index_type(index_choice);
      index_choice.memory_cost = static_cast<float>(_predict_memory_cost(index_choice));
    }
    index_choice.saved_work = _get_saved_work(index_choice);

    // Transfer results to choices vector
    choices.push_back(std::make_shared<IndexTuningOption>(index_choice));
  }
}

void AbstractIndexTuningEvaluator::_setup() {}

void AbstractIndexTuningEvaluator::_process_access_record(const AbstractIndexTuningEvaluator::AccessRecord&) {}

uintptr_t AbstractIndexTuningEvaluator::_existing_memory_cost(const IndexTuningOption& index_choice) const {
  const auto table = StorageManager::get().get_table(index_choice.column_ref.table_name);
  uintptr_t memory_cost = 0u;
  for (ChunkID chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    const auto index = chunk->get_index(index_choice.type, index_choice.column_ref.column_ids);
    if (index) {
      memory_cost += index->memory_consumption();
    }
  }
  return memory_cost;
}

std::vector<AbstractIndexTuningEvaluator::AccessRecord>
AbstractIndexTuningEvaluator::_inspect_query_cache_and_generate_access_records() {
  std::vector<AccessRecord> access_records{};

  /*
   * ToDo(anybody): The cache interface could be improved by introducing
   * some kind of accessor to the cached elements, like a values() method or an
   * iterator, to the AbstractCache interface, so this implementation could be
   * independent of the actual cache implementation.
   * See issue #756: https://github.com/hyrise/hyrise/issues/756
   */
  const auto& lqp_cache = SQLQueryCache<std::shared_ptr<AbstractLQPNode>>::get();
  // We cannot use dynamic_pointer_cast here because SQLQueryCache.cache() returns a reference, not a pointer
  auto gdfs_cache_ptr =
      dynamic_cast<const GDFSCache<std::string, std::shared_ptr<AbstractLQPNode>>*>(&(lqp_cache.cache()));
  Assert(gdfs_cache_ptr, "AbstractIndexTuningEvaluator can only analyze GDFSCache instances.");

  const auto& fibonacci_heap = gdfs_cache_ptr->queue();

  DebugAssert(fibonacci_heap.size() > 0,
              "There are no logical query plans in the cache. Make sure that logical query plans get cached!");

  auto cache_iterator = fibonacci_heap.ordered_begin();
  auto cache_end = fibonacci_heap.ordered_end();

  for (; cache_iterator != cache_end; ++cache_iterator) {
    const auto& entry = *cache_iterator;
    _inspect_lqp_node(entry.value, entry.frequency, access_records);
  }

  return access_records;
}  // namespace opossum

void AbstractIndexTuningEvaluator::_inspect_lqp_node(const std::shared_ptr<const AbstractLQPNode>& op,
                                                     size_t query_frequency,
                                                     std::vector<AccessRecord>& access_records) {
  std::list<std::shared_ptr<const AbstractLQPNode>> queue;
  queue.push_back(op);
  while (!queue.empty()) {
    auto lqp_node = queue.front();
    queue.pop_front();

    if (auto left_child = lqp_node->left_input()) {
      queue.push_back(left_child);
    }
    if (auto right_child = lqp_node->right_input()) {
      queue.push_back(right_child);
    }

    switch (lqp_node->type()) {
      case LQPNodeType::Predicate: {
        /*
         * A PredicateNode represents a scan that could possibly be sped up by
         * creating an index. To do this, we need to find out where this column
         * came from, i.e. what column on what table is scanned
         */
        auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(lqp_node);
        DebugAssert(predicate_node, "LQP node is not actually a PredicateNode");
        auto lqp_ref = predicate_node->column_reference();
        // Follow the column reference to the node "producing" it (usually a StoredTableNode)
        if (lqp_ref.original_node()) {
          auto original_node = lqp_ref.original_node();
          auto original_column_id = original_node->find_output_column_id(lqp_ref);
          if (original_node->type() == LQPNodeType::StoredTable) {
            DebugAssert(original_column_id, "Could not find column ID for LQPColumnReference");
            auto stored_table = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
            DebugAssert(stored_table, "referenced node is not actually a StoredTableNode");

            AccessRecord access_record(stored_table->table_name(), *original_column_id, query_frequency);
            access_record.condition = predicate_node->predicate_condition();
            access_record.compare_value = boost::get<AllTypeVariant>(predicate_node->value());
            access_records.push_back(access_record);
          }
        }
      } break;
      case LQPNodeType::Join:
        // Probably interesting, room for future work
        break;
      default:
        // Not interesting
        break;
    }
  }
}

std::set<ColumnRef> AbstractIndexTuningEvaluator::_aggregate_access_records(
    const std::vector<AccessRecord>& access_records) {
  std::set<ColumnRef> new_indexes{};
  for (const auto& access_record : access_records) {
    new_indexes.insert(access_record.column_ref);
    _process_access_record(access_record);
  }
  return new_indexes;
}

void AbstractIndexTuningEvaluator::_add_choices_for_existing_indexes(std::vector<IndexTuningOption>& choices,
                                                                     std::set<ColumnRef>& new_indexes) {
  for (const auto& table_name : StorageManager::get().table_names()) {
    const auto& table = StorageManager::get().get_table(table_name);

    for (const auto& index_info : table->get_indexes()) {
      auto index_choice = IndexTuningOption{ColumnRef{table_name, index_info.column_ids}, true};
      index_choice.type = index_info.type;
      choices.emplace_back(index_choice);
      // Erase this index from the set of proposed new indexes as it already exists
      new_indexes.erase({table_name, index_info.column_ids});
    }
  }
}

void AbstractIndexTuningEvaluator::_add_choices_for_new_indexes(std::vector<IndexTuningOption>& choices,
                                                                const std::set<ColumnRef>& new_indexes) {
  for (const auto& column_ref : new_indexes) {
    choices.emplace_back(column_ref, false);
  }
}

}  // namespace opossum
