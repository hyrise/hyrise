#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cache/cache.hpp"
#include "expression/expression_functional.hpp"
#include "gtest/gtest.h"
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
#include "storage/value_segment.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

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
  static std::shared_ptr<DictionarySegment<T>> create_dict_segment_by_type(
      DataType data_type, const std::vector<std::optional<T>>& values) {
    auto value_segment = std::make_shared<ValueSegment<T>>(true);

    for (const auto& value : values) {
      if (value) {
        value_segment->append(*value);
      } else {
        value_segment->append(NULL_VALUE);
      }
    }

    const auto& dict_segment =
        encode_and_compress_segment(value_segment, data_type, SegmentEncodingSpec{EncodingType::Dictionary});

    return std::static_pointer_cast<DictionarySegment<T>>(dict_segment);
  }

  void _execute_all(const std::vector<std::shared_ptr<AbstractOperator>>& operators) {
    for (auto& op : operators) {
      op->execute();
    }
  }

 public:
  /**
   * Base test uses its destructor instead of TearDown() to clean up. This way, derived test classes can override TearDown()
   * safely without preventing the BaseTest-cleanup from happening.
   * GTest runs the destructor right after TearDown(): https://github.com/abseil/googletest/blob/master/googletest/docs/faq.md#should-i-use-the-constructordestructor-of-the-test-fixture-or-setupteardown
   */
  ~BaseTestWithParam() {
    Hyrise::reset();
    SQLPipelineBuilder::default_pqp_cache = nullptr;
    SQLPipelineBuilder::default_lqp_cache = nullptr;
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
    } else if (is_between_predicate_condition(predicate_condition)) {
      return create_between_table_scan(in, column_id, value, value2, predicate_condition);
    } else {
      predicate = std::make_shared<BinaryPredicateExpression>(predicate_condition, column_expression, value_(value));
    }

    return std::make_shared<TableScan>(in, predicate);
  }

  static void set_statistics_for_mock_node(
      const std::shared_ptr<MockNode>& mock_node, const size_t row_count,
      const std::vector<std::shared_ptr<AbstractStatisticsObject>>& statistics_objects) {
    const auto& column_definitions = mock_node->column_definitions();
    auto output_column_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>(column_definitions.size());

    for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
      resolve_data_type(column_definitions[column_id].first, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        const auto column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
        column_statistics->set_statistics_object(statistics_objects[column_id]);
        output_column_statistics[column_id] = column_statistics;
      });
    }

    const auto table_statistics = std::make_shared<TableStatistics>(std::move(output_column_statistics), row_count);
    mock_node->set_table_statistics(table_statistics);
  }

  static std::shared_ptr<MockNode> create_mock_node_with_statistics(
      const MockNode::ColumnDefinitions& column_definitions, const size_t row_count,
      const std::vector<std::shared_ptr<AbstractStatisticsObject>>& statistics_objects) {
    Assert(column_definitions.size() == statistics_objects.size(), "Column count mismatch");

    const auto mock_node = MockNode::make(column_definitions);

    set_statistics_for_mock_node(mock_node, row_count, statistics_objects);

    return mock_node;
  }

  // Utility to create between table scans
  static std::shared_ptr<TableScan> create_between_table_scan(const std::shared_ptr<AbstractOperator>& in,
                                                              const ColumnID column_id, const AllTypeVariant& value,
                                                              const std::optional<AllTypeVariant>& value2,
                                                              const PredicateCondition predicate_condition) {
    const auto column_expression = get_column_expression(in, column_id);
    const auto predicate =
        std::make_shared<BetweenExpression>(predicate_condition, column_expression, value_(value), value_(*value2));

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

  static void assert_chunk_encoding(const std::shared_ptr<Chunk>& chunk, const ChunkEncodingSpec& spec) {
    const auto column_count = chunk->column_count();
    for (auto column_id = ColumnID{0u}; column_id < column_count; ++column_id) {
      const auto segment = chunk->get_segment(column_id);
      const auto segment_spec = spec.at(column_id);

      if (segment_spec.encoding_type == EncodingType::Unencoded) {
        const auto value_segment = std::dynamic_pointer_cast<const BaseValueSegment>(segment);
        ASSERT_NE(value_segment, nullptr);
      } else {
        const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
        ASSERT_NE(encoded_segment, nullptr);
        ASSERT_EQ(encoded_segment->encoding_type(), segment_spec.encoding_type);
        if (segment_spec.vector_compression_type && encoded_segment->compressed_vector_type()) {
          // Both optionals need to be set for comparison, because some encodings only use vector compression for
          // certain types (e.g., LZ4 for pmr_string segments).
          ASSERT_EQ(*segment_spec.vector_compression_type,
                    parent_vector_compression_type(*encoded_segment->compressed_vector_type()));
        }
      }
    }
  }

  static std::vector<SegmentEncodingSpec> get_supporting_segment_encodings_specs(const DataType data_type,
                                                                                 const bool include_unencoded = true) {
    std::vector<SegmentEncodingSpec> segment_encodings;
    for (const auto& spec : all_segment_encoding_specs) {
      // Add all encoding types to the returned vector if they support the given data type. As some test cases work on
      // encoded segments only, it is further tested if the segment is not encoded and if segments of type Unencoded
      // should be included or not (flag `include_unencoded`).
      if (encoding_supports_data_type(spec.encoding_type, data_type) &&
          (spec.encoding_type != EncodingType::Unencoded || include_unencoded)) {
        segment_encodings.emplace_back(spec);
      }
    }
    return segment_encodings;
  }
};

using BaseTest = BaseTestWithParam<void>;

}  // namespace opossum
