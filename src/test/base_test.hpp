#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "cache/cache.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/table_scan.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/abstract_statistics_object.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

using namespace expression_functional;  // NOLINT

class AbstractLQPNode;

extern std::string test_data_path;

template <typename ParamType>
class BaseTestWithParam
    : public std::conditional_t<std::is_same_v<ParamType, void>, ::testing::Test, ::testing::TestWithParam<ParamType>> {
 public:
  /**
   * Base test uses its destructor instead of TearDown() to clean up. This way, derived test classes can override TearDown()
   * safely without preventing the BaseTest-cleanup from happening.
   * GTest runs the destructor right after TearDown(): https://github.com/abseil/googletest/blob/master/googletest/docs/faq.md#should-i-use-the-constructordestructor-of-the-test-fixture-or-setupteardown
   */
  ~BaseTestWithParam() { Hyrise::reset(); }
};

using BaseTest = BaseTestWithParam<void>;

// creates a dictionary segment with the given type and values
template <typename T>
std::shared_ptr<DictionarySegment<T>> create_dict_segment_by_type(DataType data_type,
                                                                  const std::vector<std::optional<T>>& values);

void execute_all(const std::vector<std::shared_ptr<AbstractOperator>>& operators);

std::shared_ptr<AbstractExpression> get_column_expression(const std::shared_ptr<AbstractOperator>& op,
                                                          const ColumnID column_id);

// Utility to create table scans
std::shared_ptr<TableScan> create_table_scan(const std::shared_ptr<AbstractOperator>& in, const ColumnID column_id,
                                             const PredicateCondition predicate_condition, const AllTypeVariant& value,
                                             const std::optional<AllTypeVariant>& value2 = std::nullopt);

void set_statistics_for_mock_node(const std::shared_ptr<MockNode>& mock_node, const size_t row_count,
                                  const std::vector<std::shared_ptr<AbstractStatisticsObject>>& statistics_objects);

std::shared_ptr<MockNode> create_mock_node_with_statistics(
    const MockNode::ColumnDefinitions& column_definitions, const size_t row_count,
    const std::vector<std::shared_ptr<AbstractStatisticsObject>>& statistics_objects);

// Utility to create between table scans
std::shared_ptr<TableScan> create_between_table_scan(const std::shared_ptr<AbstractOperator>& in,
                                                     const ColumnID column_id, const AllTypeVariant& value,
                                                     const std::optional<AllTypeVariant>& value2,
                                                     const PredicateCondition predicate_condition);

ChunkEncodingSpec create_compatible_chunk_encoding_spec(const Table& table,
                                                        const SegmentEncodingSpec& desired_segment_encoding);

void assert_chunk_encoding(const std::shared_ptr<Chunk>& chunk, const ChunkEncodingSpec& spec);

std::vector<SegmentEncodingSpec> get_supporting_segment_encodings_specs(const DataType data_type,
                                                                        const bool include_unencoded = true);

bool file_exists(const std::string& name);

bool compare_files(const std::string& original_file, const std::string& created_file);

}  // namespace opossum
