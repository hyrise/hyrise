#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cache/cache.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "gtest/gtest.h"
#include "operators/abstract_operator.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"

namespace opossum {

using namespace expression_functional;  // NOLINT

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
  BaseTestWithParam() {}

  /**
   * Base test uses its destructor instead of TearDown() to clean up. This way, derived test classes can override TearDown()
   * safely without preventing the BaseTest-cleanup from happening.
   * GTest runs the destructor right after TearDown(): https://github.com/abseil/googletest/blob/master/googletest/docs/faq.md#should-i-use-the-constructordestructor-of-the-test-fixture-or-setupteardown
   */
  ~BaseTestWithParam() {
    // Reset scheduler first so that all tasks are done before we kill the StorageManager
    CurrentScheduler::set(nullptr);

    PluginManager::reset();
    StorageManager::reset();
    TransactionManager::reset();

    SQLPhysicalPlanCache::get().clear();
    SQLLogicalPlanCache::get().clear();
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

  static ChunkEncodingSpec create_compatible_chunk_encoding_spec(const Table& table,
                                                                 const SegmentEncodingSpec& desired_segment_encoding) {
    auto chunk_encoding_spec = ChunkEncodingSpec{table.column_count(), EncodingType::Unencoded};

    for (auto column_id = ColumnID{0}; column_id < table.column_count(); ++column_id) {
      if (encoding_supports_data_type(desired_segment_encoding.encoding_type, table.column_data_type(column_id))) {
        chunk_encoding_spec[column_id] = desired_segment_encoding;
      }
    }

    return chunk_encoding_spec;
  }
};

using BaseTest = BaseTestWithParam<void>;

}  // namespace opossum
