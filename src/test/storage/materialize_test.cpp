#include "gtest/gtest.h"

#include "encoding_test.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/materialize.hpp"
#include "storage/table.hpp"

namespace opossum {

class MaterializeTest : public EncodingTest {
 public:
  void SetUp() override {
    _data_table = load_table_with_encoding("src/test/tables/int_float.tbl", 2);
    _data_table_with_nulls = load_table_with_encoding("src/test/tables/int_float_with_null.tbl", 2);

    const auto table_wrapper = std::make_shared<TableWrapper>(_data_table);
    table_wrapper->execute();

    const auto table_scan = std::make_shared<TableScan>(table_wrapper, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    table_scan->execute();
    _references_table = table_scan->get_output();
  }

  template <typename T>
  std::vector<T> materialize_values_to_vector(const BaseColumn& column) {
    std::vector<T> values;
    materialize_values(column, values);
    return values;
  }

  template <typename T>
  std::vector<std::optional<T>> materialize_values_and_nulls_to_vector(const BaseColumn& column) {
    std::vector<std::optional<T>> values_and_nulls;
    materialize_values_and_nulls(column, values_and_nulls);
    return values_and_nulls;
  }

  template <typename T>
  std::vector<bool> materialize_nulls_to_vector(const BaseColumn& column) {
    std::vector<bool> nulls;
    materialize_nulls<T>(column, nulls);
    return nulls;
  }

  std::shared_ptr<const Table> _data_table;
  std::shared_ptr<const Table> _references_table;
  std::shared_ptr<const Table> _data_table_with_nulls;
};

TEST_P(MaterializeTest, MaterializeIntData) {
  EXPECT_EQ(std::vector<int32_t>({12345, 123}),
            materialize_values_to_vector<int32_t>(*_data_table->get_chunk(ChunkID(0))->get_column(ColumnID(0))));
  EXPECT_EQ(std::vector<int32_t>({1234}),
            materialize_values_to_vector<int32_t>(*_data_table->get_chunk(ChunkID(1))->get_column(ColumnID(0))));
}

TEST_P(MaterializeTest, MaterializeIntReferences) {
  EXPECT_EQ(std::vector<int32_t>({12345, 123}),
            materialize_values_to_vector<int32_t>(*_references_table->get_chunk(ChunkID(0))->get_column(ColumnID(0))));
  EXPECT_EQ(std::vector<int32_t>({1234}),
            materialize_values_to_vector<int32_t>(*_references_table->get_chunk(ChunkID(1))->get_column(ColumnID(0))));
}

TEST_P(MaterializeTest, MaterializeFloatData) {
  const auto floats_0 =
      materialize_values_to_vector<float>(*_data_table->get_chunk(ChunkID(0))->get_column(ColumnID(1)));
  ASSERT_EQ(floats_0.size(), 2u);
  EXPECT_FLOAT_EQ(458.7, floats_0.at(0));
  EXPECT_FLOAT_EQ(456.7f, floats_0.at(1));

  const auto floats_1 =
      materialize_values_to_vector<float>(*_data_table->get_chunk(ChunkID(1))->get_column(ColumnID(1)));
  ASSERT_EQ(floats_1.size(), 1u);
  EXPECT_FLOAT_EQ(457.7, floats_1.at(0));
}

TEST_P(MaterializeTest, MaterializeFloatReferences) {
  const auto floats_0 =
      materialize_values_to_vector<float>(*_references_table->get_chunk(ChunkID(0))->get_column(ColumnID(1)));
  ASSERT_EQ(floats_0.size(), 2u);
  EXPECT_FLOAT_EQ(458.7, floats_0.at(0));
  EXPECT_FLOAT_EQ(456.7f, floats_0.at(1));

  const auto floats_1 =
      materialize_values_to_vector<float>(*_references_table->get_chunk(ChunkID(1))->get_column(ColumnID(1)));
  ASSERT_EQ(floats_1.size(), 1u);
  EXPECT_FLOAT_EQ(457.7, floats_1.at(0));
}

TEST_P(MaterializeTest, MaterializeValuesAndNulls) {
  const auto values_and_nulls = materialize_values_and_nulls_to_vector<int32_t>(
      *_data_table_with_nulls->get_chunk(ChunkID(1))->get_column(ColumnID(0)));
  auto expected = std::vector<std::optional<int32_t>>{};
  expected.emplace_back(std::nullopt);
  expected.emplace_back(1234);

  EXPECT_EQ(expected, values_and_nulls);
}

TEST_P(MaterializeTest, MaterializeNulls) {
  const auto nulls =
      materialize_nulls_to_vector<float>(*_data_table_with_nulls->get_chunk(ChunkID(0))->get_column(ColumnID(1)));
  auto expected = std::vector<bool>{false, true};

  EXPECT_EQ(expected, nulls);
}

INSTANCIATE_ENCODING_TYPE_TESTS(MaterializeTest);

}  // namespace opossum
