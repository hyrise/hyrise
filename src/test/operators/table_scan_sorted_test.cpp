#include "base_test.hpp"
#include "gtest/gtest.h"
#include "operators/table_wrapper.hpp"

namespace opossum {

using Params =
    std::tuple<EncodingType, bool, std::pair<PredicateCondition, std::vector<AllTypeVariant>>, DataType, bool>;

class OperatorsTableScanSortedTest : public BaseTest, public ::testing::WithParamInterface<Params> {
 protected:
  void SetUp() override {
    _encoding_type = std::get<0>(GetParam());
    _data_type = std::get<3>(GetParam());
    _ascending = std::get<4>(GetParam());
    _expected = std::get<2>(GetParam()).second;

    if (!_ascending) {
      std::reverse(_expected.begin(), _expected.end());
    }

    // TODO(cmfcmf): Test with null values
    _table_column_definitions.emplace_back("nulls_first", _data_type, true);
    _table_column_definitions.emplace_back("nulls_last", _data_type, true);

    _table = std::make_shared<Table>(_table_column_definitions, TableType::Data);

    const int table_size = 10;
    for (int i = 0; i < table_size; i++) {
      if (_data_type == DataType::Int) {
        if (_ascending) {
          _table->append({i, i});
        } else {
          _table->append({table_size - i - 1, table_size - i - 1});
        }
      } else if (_data_type == DataType::String) {
        if (_ascending) {
          const auto str = std::to_string(i);
          _table->append({str, str});
        } else {
          const auto str = std::to_string(table_size - i - 1);
          _table->append({str, str});
        }
      } else {
        Fail("Unsupported DataType");
      }
    }

    ChunkEncoder::encode_all_chunks(_table, SegmentEncodingSpec(_encoding_type));

    _table->get_chunk(ChunkID(0))
        ->get_segment(ColumnID(0))
        ->set_sort_order(_ascending ? OrderByMode::Ascending : OrderByMode::Descending);
    _table->get_chunk(ChunkID(0))
        ->get_segment(ColumnID(1))
        ->set_sort_order(_ascending ? OrderByMode::AscendingNullsLast : OrderByMode::DescendingNullsLast);

    _table_wrapper = std::make_shared<TableWrapper>(std::move(_table));
    _table_wrapper->execute();
  }

  void ASSERT_COLUMN_SORTED_EQ(std::shared_ptr<const Table> table, const ColumnID& column_id) {
    size_t i = 0;
    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk->size(); ++chunk_offset, ++i) {
        const auto& segment = *chunk->get_segment(column_id);

        const auto found_value = segment[chunk_offset];

        if (_data_type == DataType::String) {
          ASSERT_EQ(type_cast_variant<std::string>(_expected[i]), type_cast_variant<std::string>(found_value));
        } else {
          ASSERT_EQ(_expected[i], found_value);
        }
      }
    }

    ASSERT_EQ(_expected.size(), i);
  }

 protected:
  std::shared_ptr<Table> _table;
  std::shared_ptr<TableWrapper> _table_wrapper;
  TableColumnDefinitions _table_column_definitions;
  EncodingType _encoding_type;
  DataType _data_type;
  bool _ascending;
  std::vector<AllTypeVariant> _expected;
};

auto formatter = [](const ::testing::TestParamInfo<Params> info) {
  return (std::get<1>(info.param) ? "Reference" : "Direct") + data_type_to_string.left.at(std::get<3>(info.param)) +
         encoding_type_to_string.left.at(std::get<0>(info.param)) +
         (std::get<4>(info.param) ? "Ascending" : "Descending") +
         predicate_condition_to_string_readable.left.at(std::get<2>(info.param).first);
};

INSTANTIATE_TEST_CASE_P(
    EncodingTypes, OperatorsTableScanSortedTest,
    ::testing::Combine(
        ::testing::Values(EncodingType::Unencoded,
                          EncodingType::Dictionary /*, EncodingType::RunLength, EncodingType::FrameOfReference*/),
        ::testing::Bool(),
        ::testing::Values(
            std::pair<PredicateCondition, std::vector<AllTypeVariant>>(PredicateCondition::Equals, {5}),
            std::pair<PredicateCondition, std::vector<AllTypeVariant>>(PredicateCondition::NotEquals,
                                                                       {0, 1, 2, 3, 4, 6, 7, 8, 9}),
            std::pair<PredicateCondition, std::vector<AllTypeVariant>>(PredicateCondition::LessThan, {0, 1, 2, 3, 4}),
            std::pair<PredicateCondition, std::vector<AllTypeVariant>>(PredicateCondition::LessThanEquals,
                                                                       {0, 1, 2, 3, 4, 5}),
            std::pair<PredicateCondition, std::vector<AllTypeVariant>>(PredicateCondition::GreaterThan, {6, 7, 8, 9}),
            std::pair<PredicateCondition, std::vector<AllTypeVariant>>(PredicateCondition::GreaterThanEquals,
                                                                       {5, 6, 7, 8, 9})),
        ::testing::Values(DataType::Int, DataType::String), ::testing::Bool()),
    formatter);

TEST_P(OperatorsTableScanSortedTest, TestSortedScan) {
  const bool use_reference_segment = std::get<1>(GetParam());

  for (auto column_index = ColumnID{0}; column_index < 2; ++column_index) {
    const auto column_definition = _table_column_definitions[column_index];
    const auto column_expression =
        pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

    const auto predicate =
        std::make_shared<BinaryPredicateExpression>(std::get<2>(GetParam()).first, column_expression, value_(5));

    if (use_reference_segment) {
      const auto dummy_predicate =
          std::make_shared<BinaryPredicateExpression>(PredicateCondition::NotEquals, column_expression, value_(-1));
      auto dummy = std::make_shared<TableScan>(_table_wrapper, dummy_predicate);
      dummy->execute();

      auto scan = std::make_shared<TableScan>(dummy, predicate);
      scan->execute();
      ASSERT_COLUMN_SORTED_EQ(scan->get_output(), column_index);
    } else {
      auto scan = std::make_shared<TableScan>(_table_wrapper, predicate);
      scan->execute();
      ASSERT_COLUMN_SORTED_EQ(scan->get_output(), column_index);
    }
  }
}

}  // namespace opossum
