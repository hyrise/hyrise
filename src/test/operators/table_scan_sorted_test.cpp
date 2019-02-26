#include "base_test.hpp"
#include "gtest/gtest.h"
#include "operators/limit.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

using Params = std::tuple<EncodingType, bool, std::pair<PredicateCondition, std::vector<AllTypeVariant>>, DataType,
                          OrderByMode, bool>;

class OperatorsTableScanSortedTest : public BaseTest, public ::testing::WithParamInterface<Params> {
 protected:
  void SetUp() override {
    _encoding_type = std::get<0>(GetParam());
    _data_type = std::get<3>(GetParam());
    _order_by = std::get<4>(GetParam());
    _expected = std::get<2>(GetParam()).second;
    const bool nullable = std::get<5>(GetParam());
    const bool use_reference_segment = std::get<1>(GetParam());

    const bool ascending = _order_by == OrderByMode::Ascending || _order_by == OrderByMode::AscendingNullsLast;
    const bool nulls_first = _order_by == OrderByMode::Ascending || _order_by == OrderByMode::Descending;

    if (!ascending) {
      std::reverse(_expected.begin(), _expected.end());
    }

    _table_column_definitions.emplace_back("a", _data_type, nullable);

    auto table = Table::create_dummy_table(_table_column_definitions);

    if (nullable && nulls_first) {
      table->append({NULL_VALUE});
      table->append({NULL_VALUE});
      table->append({NULL_VALUE});
    }

    const int table_size = 10;
    for (int i = 0; i < table_size; i++) {
      if (_data_type == DataType::Int) {
        table->append({ascending ? i : table_size - i - 1});
        if (use_reference_segment) {
          table->append({ascending ? i : table_size - i - 1});
        }
      } else if (_data_type == DataType::String) {
        table->append({std::to_string(ascending ? i : table_size - i - 1)});
        if (use_reference_segment) {
          table->append({std::to_string(ascending ? i : table_size - i - 1)});
        }
      } else {
        Fail("Unsupported DataType");
      }
    }

    if (nullable && !nulls_first) {
      table->append({NULL_VALUE});
      table->append({NULL_VALUE});
      table->append({NULL_VALUE});
    }

    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec(_encoding_type));

    table->get_chunk(ChunkID(0))->get_segment(ColumnID(0))->set_sort_order(_order_by);

    if (use_reference_segment) {
      auto pos_list = std::make_shared<PosList>();

      if (nullable && nulls_first) {
        pos_list->emplace_back(INVALID_CHUNK_ID, INVALID_CHUNK_OFFSET);
        pos_list->emplace_back(INVALID_CHUNK_ID, INVALID_CHUNK_OFFSET);
      }

      for (auto i = 0; i < 2 * table_size; ++i) {
        if (i % 2 == 0) {
          pos_list->emplace_back(ChunkID(0), i + (nullable && nulls_first ? 3 : 0));
        }
      }

      if (nullable && !nulls_first) {
        pos_list->emplace_back(INVALID_CHUNK_ID, INVALID_CHUNK_OFFSET);
        pos_list->emplace_back(INVALID_CHUNK_ID, INVALID_CHUNK_OFFSET);
      }

      const auto reference_segment = std::make_shared<ReferenceSegment>(table, ColumnID(0), pos_list);

      auto reference_table = std::make_shared<Table>(_table_column_definitions, TableType::References);
      reference_table->append_chunk({reference_segment});

      _table_wrapper = std::make_shared<TableWrapper>(std::move(reference_table));
    } else {
      _table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    }

    _table_wrapper->execute();
  }

  void ASSERT_COLUMN_SORTED_EQ(std::shared_ptr<const Table> table) {
    ASSERT_EQ(table->row_count(), _expected.size());

    size_t i = 0;
    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk->size(); ++chunk_offset, ++i) {
        const auto& segment = *chunk->get_segment(ColumnID(0));

        const auto found_value = segment[chunk_offset];

        ASSERT_FALSE(variant_is_null(found_value)) << "row " << i << " is null";
        if (_data_type == DataType::String) {
          ASSERT_EQ(type_cast_variant<std::string>(found_value), type_cast_variant<std::string>(_expected[i]))
              << "row " << i << " invalid";
        } else {
          ASSERT_EQ(found_value, _expected[i]) << "row " << i << " invalid";
        }
      }
    }
  }

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper;
  TableColumnDefinitions _table_column_definitions;
  EncodingType _encoding_type;
  DataType _data_type;
  OrderByMode _order_by;
  std::vector<AllTypeVariant> _expected;
};

auto formatter = [](const ::testing::TestParamInfo<Params> info) {
  return (std::get<1>(info.param) ? "Reference" : "Direct") + data_type_to_string.left.at(std::get<3>(info.param)) +
         encoding_type_to_string.left.at(std::get<0>(info.param)) +
         order_by_mode_to_string.at(std::get<4>(info.param)) +
         predicate_condition_to_string_readable.left.at(std::get<2>(info.param).first) +
         (std::get<5>(info.param) ? "WithNulls" : "WithoutNulls");
};

// TODO(cmfcmf): Test all operators with data where the result is expected to be empty.
INSTANTIATE_TEST_CASE_P(
    EncodingTypes, OperatorsTableScanSortedTest,
    ::testing::Combine(
        ::testing::Values(EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::RunLength
                          /*EncodingType::FrameOfReference, EncodingType::FixedStringDictionary*/),
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
        ::testing::Values(DataType::Int, DataType::String),
        ::testing::Values(OrderByMode::Ascending, OrderByMode::AscendingNullsLast, OrderByMode::Descending,
                          OrderByMode::DescendingNullsLast),
        ::testing::Bool()),
    formatter);

TEST_P(OperatorsTableScanSortedTest, TestSortedScan) {
  const auto column_definition = _table_column_definitions[0];
  const auto column_expression =
      pqp_column_(ColumnID(0), column_definition.data_type, column_definition.nullable, column_definition.name);

  const auto predicate =
      std::make_shared<BinaryPredicateExpression>(std::get<2>(GetParam()).first, column_expression, value_(5));

  auto scan = std::make_shared<TableScan>(_table_wrapper, predicate);
  scan->execute();
  ASSERT_COLUMN_SORTED_EQ(scan->get_output());
}

}  // namespace opossum
