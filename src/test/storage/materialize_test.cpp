#include "gtest/gtest.h"

#include "encoding_test.hpp"
#include "expression/expression_functional.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/materialize.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class MaterializeTest : public EncodingTest {
 public:
  void SetUp() override {
    _data_table = load_table_with_encoding("resources/test_data/tbl/int_float.tbl", 2);
    _data_table_with_nulls = load_table_with_encoding("resources/test_data/tbl/int_float_with_null.tbl", 2);

    const auto table_wrapper = std::make_shared<TableWrapper>(_data_table);
    table_wrapper->execute();

    const auto a = PQPColumnExpression::from_table(*_data_table, "a");
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, greater_than_(a, 0));

    table_scan->execute();
    _references_table = table_scan->get_output();
  }

  template <typename T>
  std::vector<T> materialize_values_to_vector(const BaseSegment& segment) {
    std::vector<T> values;
    materialize_values(segment, values);
    return values;
  }

  template <typename T>
  std::vector<std::pair<bool, T>> materialize_values_and_nulls_to_vector(const BaseSegment& segment) {
    std::vector<std::pair<bool, T>> values_and_nulls;
    materialize_values_and_nulls(segment, values_and_nulls);
    return values_and_nulls;
  }

  template <typename T>
  std::vector<bool> materialize_nulls_to_vector(const BaseSegment& segment) {
    std::vector<bool> nulls;
    materialize_nulls<T>(segment, nulls);
    return nulls;
  }

  std::shared_ptr<const Table> _data_table;
  std::shared_ptr<const Table> _references_table;
  std::shared_ptr<const Table> _data_table_with_nulls;
};

TEST_P(MaterializeTest, MaterializeIntData) {
  EXPECT_EQ(std::vector<int32_t>({12345, 123}),
            materialize_values_to_vector<int32_t>(*_data_table->get_chunk(ChunkID(0))->get_segment(ColumnID(0))));
  EXPECT_EQ(std::vector<int32_t>({1234}),
            materialize_values_to_vector<int32_t>(*_data_table->get_chunk(ChunkID(1))->get_segment(ColumnID(0))));
}

TEST_P(MaterializeTest, MaterializeTwoIntSegments) {
  std::vector<int32_t> values;
  materialize_values(*_data_table->get_chunk(ChunkID(0))->get_segment(ColumnID(0)), values);
  materialize_values(*_data_table->get_chunk(ChunkID(1))->get_segment(ColumnID(0)), values);
  EXPECT_EQ(values, (std::vector<int32_t>{12345, 123, 1234}));
}

TEST_P(MaterializeTest, MaterializeIntReferences) {
  EXPECT_EQ(std::vector<int32_t>({12345, 123}),
            materialize_values_to_vector<int32_t>(*_references_table->get_chunk(ChunkID(0))->get_segment(ColumnID(0))));
  EXPECT_EQ(std::vector<int32_t>({1234}),
            materialize_values_to_vector<int32_t>(*_references_table->get_chunk(ChunkID(1))->get_segment(ColumnID(0))));
}

TEST_P(MaterializeTest, MaterializeTwoIntReferenceSegments) {
  std::vector<int32_t> values;
  materialize_values(*_references_table->get_chunk(ChunkID(1))->get_segment(ColumnID(0)), values);
  materialize_values(*_references_table->get_chunk(ChunkID(0))->get_segment(ColumnID(0)), values);
  EXPECT_EQ(values, (std::vector<int32_t>{1234, 12345, 123}));
}

TEST_P(MaterializeTest, MaterializeFloatData) {
  const auto floats_0 =
      materialize_values_to_vector<float>(*_data_table->get_chunk(ChunkID(0))->get_segment(ColumnID(1)));
  ASSERT_EQ(floats_0.size(), 2u);
  EXPECT_FLOAT_EQ(458.7, floats_0.at(0));
  EXPECT_FLOAT_EQ(456.7f, floats_0.at(1));

  const auto floats_1 =
      materialize_values_to_vector<float>(*_data_table->get_chunk(ChunkID(1))->get_segment(ColumnID(1)));
  ASSERT_EQ(floats_1.size(), 1u);
  EXPECT_FLOAT_EQ(457.7, floats_1.at(0));
}

TEST_P(MaterializeTest, MaterializeFloatReferences) {
  const auto floats_0 =
      materialize_values_to_vector<float>(*_references_table->get_chunk(ChunkID(0))->get_segment(ColumnID(1)));
  ASSERT_EQ(floats_0.size(), 2u);
  EXPECT_FLOAT_EQ(458.7, floats_0.at(0));
  EXPECT_FLOAT_EQ(456.7f, floats_0.at(1));

  const auto floats_1 =
      materialize_values_to_vector<float>(*_references_table->get_chunk(ChunkID(1))->get_segment(ColumnID(1)));
  ASSERT_EQ(floats_1.size(), 1u);
  EXPECT_FLOAT_EQ(457.7, floats_1.at(0));
}

TEST_P(MaterializeTest, MaterializeValuesAndNulls) {
  const auto values_and_nulls = materialize_values_and_nulls_to_vector<int32_t>(
      *_data_table_with_nulls->get_chunk(ChunkID(1))->get_segment(ColumnID(0)));

  ASSERT_EQ(values_and_nulls.size(), 2u);
  EXPECT_EQ(values_and_nulls[0].first, true);
  EXPECT_EQ(values_and_nulls[1].first, false);
  EXPECT_EQ(values_and_nulls[1].second, 1234);
}

TEST_P(MaterializeTest, MaterializeValuesAndNullsTwoSegments) {
  std::vector<std::pair<bool, int32_t>> values_and_nulls;
  materialize_values_and_nulls(*_data_table_with_nulls->get_chunk(ChunkID(0))->get_segment(ColumnID(0)),
                               values_and_nulls);
  materialize_values_and_nulls(*_data_table_with_nulls->get_chunk(ChunkID(1))->get_segment(ColumnID(0)),
                               values_and_nulls);
  ASSERT_EQ(values_and_nulls.size(), 4u);
  EXPECT_EQ(values_and_nulls[0].first, false);
  EXPECT_EQ(values_and_nulls[0].second, 12345);
  EXPECT_EQ(values_and_nulls[1].first, false);
  EXPECT_EQ(values_and_nulls[1].second, 123);
  EXPECT_EQ(values_and_nulls[2].first, true);
  EXPECT_EQ(values_and_nulls[3].first, false);
  EXPECT_EQ(values_and_nulls[3].second, 1234);
}

TEST_P(MaterializeTest, MaterializeNulls) {
  const auto nulls =
      materialize_nulls_to_vector<float>(*_data_table_with_nulls->get_chunk(ChunkID(0))->get_segment(ColumnID(1)));
  auto expected = std::vector<bool>{false, true};

  EXPECT_EQ(expected, nulls);
}

TEST_P(MaterializeTest, MaterializeNullsTwoSegments) {
  std::vector<bool> nulls;
  materialize_nulls<float>(*_data_table_with_nulls->get_chunk(ChunkID(0))->get_segment(ColumnID(1)), nulls);
  materialize_nulls<float>(*_data_table_with_nulls->get_chunk(ChunkID(1))->get_segment(ColumnID(1)), nulls);
  auto expected = std::vector<bool>{false, true, false, false};

  EXPECT_EQ(expected, nulls);
}

INSTANTIATE_TEST_SUITE_P(MaterializeTestInstances, MaterializeTest, ::testing::ValuesIn(all_segment_encoding_specs));

}  // namespace opossum
