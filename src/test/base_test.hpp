#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "gtest/gtest.h"
#include "operators/abstract_operator.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/materialize.hpp"
#include "storage/numa_placement_manager.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class AbstractLQPNode;
class Table;

extern std::string test_data_path;

template <typename ParamType>
class BaseTestWithParam
    : public std::conditional_t<std::is_same_v<ParamType, void>, ::testing::Test, ::testing::TestWithParam<ParamType>> {
 protected:
  // creates a dictionary segment with the given type and values
  template <typename T>
  static std::shared_ptr<DictionarySegment<T>> create_dict_segment_by_type(DataType data_type,
                                                                           const std::vector<T>& values) {
    auto vector_values = tbb::concurrent_vector<T>(values.begin(), values.end());
    auto value_segment = std::make_shared<ValueSegment<T>>(std::move(vector_values));

    auto compressed_segment = encode_segment(EncodingType::Dictionary, data_type, value_segment);
    return std::static_pointer_cast<DictionarySegment<T>>(compressed_segment);
  }

  void _execute_all(const std::vector<std::shared_ptr<AbstractOperator>>& operators) {
    for (auto& op : operators) {
      op->execute();
    }
  }

 public:
  BaseTestWithParam() {
#if HYRISE_NUMA_SUPPORT
    // Set options with very short cycle times
    auto options = NUMAPlacementManager::Options();
    options.counter_history_interval = std::chrono::milliseconds(1);
    options.counter_history_range = std::chrono::milliseconds(70);
    options.migration_interval = std::chrono::milliseconds(100);
    NUMAPlacementManager::get().set_options(options);
#endif
  }

  ~BaseTestWithParam() {
    // Reset scheduler first so that all tasks are done before we kill the StorageManager
    CurrentScheduler::set(nullptr);

#if HYRISE_NUMA_SUPPORT
    // Also make sure that the tasks in the NUMAPlacementManager are not running anymore. We don't restart it here.
    // If you want the NUMAPlacementManager in your test, start it yourself. This is to prevent migrations where we
    // don't expect them
    NUMAPlacementManager::get().pause();
#endif

    PluginManager::reset();
    StorageManager::reset();
    TransactionManager::reset();
    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  static std::shared_ptr<AbstractExpression> get_column_expression(const std::shared_ptr<AbstractOperator>& op,
                                                                   const ColumnID column_id) {
    Assert(op->get_output(), "Expected Operator to be executed");
    const auto output_table = op->get_output();
    const auto& column_definition = output_table->column_definitions().at(column_id);

    return pqp_column_(column_id, column_definition.data_type, column_definition.nullable, column_definition.name);
  }

  // Utility to create table scans
  static std::shared_ptr<TableScan> create_table_scan(const std::shared_ptr<AbstractOperator>& in,
                                                      const ColumnID column_id,
                                                      const PredicateCondition predicate_condition,
                                                      const AllTypeVariant& value,
                                                      const std::optional<AllTypeVariant>& value2 = std::nullopt) {
    const auto column_expression = get_column_expression(in, column_id);

    auto predicate = std::shared_ptr<AbstractExpression>{};
    if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
      predicate = std::make_shared<IsNullExpression>(predicate_condition, column_expression);
    } else if (predicate_condition == PredicateCondition::Between) {
      Assert(value2, "Need value2 for BetweenExpression");
      predicate = std::make_shared<BetweenExpression>(column_expression, value_(value), value_(*value2));
    } else {
      predicate = std::make_shared<BinaryPredicateExpression>(predicate_condition, column_expression, value_(value));
    }

    return std::make_shared<TableScan>(in, predicate);
  }

  static std::shared_ptr<Table> load_table_cached(const std::string& file_name, size_t chunk_size = Chunk::MAX_SIZE) {
    auto cache_iter = table_cache.find({file_name, chunk_size});

    if (cache_iter == table_cache.end()) {
      cache_iter = table_cache.emplace(std::make_pair(file_name, chunk_size), load_table(file_name, chunk_size)).first;
    }

    const auto cached_table = cache_iter->second;

    const auto result_table = std::make_shared<Table>(cached_table->column_definitions(), TableType::Data, cached_table->max_chunk_size());

    for (const auto& cached_chunk : cached_table->chunks()) {
      auto segments = Segments{cached_table->column_count()};

      for (auto column_id = ColumnID{0}; column_id < cached_table->column_count(); ++column_id) {
        const auto cached_segment = cached_chunk->get_segment(column_id);
        const auto cached_value_segment = std::dynamic_pointer_cast<BaseValueSegment>(cached_segment);
        Assert(cached_value_segment, "Expected ValueSegment in cache");

        resolve_data_type(cached_table->column_data_type(column_id), [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;

          pmr_concurrent_vector<ColumnDataType> values;
          values.reserve(cached_segment->size());
          materialize_values<ColumnDataType>(*cached_segment, values);

          if (cached_value_segment->is_nullable()) {
            pmr_concurrent_vector<bool> null_values;
            null_values.reserve(cached_segment->size());
            materialize_nulls<ColumnDataType>(*cached_segment, null_values);

            segments[column_id] = std::make_shared<ValueSegment>(std::move(values), std::move(null_values));
          } else {

            segments[column_id] = std::make_shared<ValueSegment>(std::move(values));
          }
        });

      }
    }

    return result_table;
  }

  static std::map<std::pair<std::string, ChunkOffset>, std::shared_ptr<Table>> table_cache;
};

using BaseTest = BaseTestWithParam<void>;

}  // namespace opossum
