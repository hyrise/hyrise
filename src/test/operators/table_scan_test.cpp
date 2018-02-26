#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsTableScanTest : public BaseTest, public ::testing::WithParamInterface<EncodingType> {
 protected:
  void SetUp() override { _encoding_type = GetParam(); }

  std::shared_ptr<TableWrapper> get_table_op() {
    auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    table_wrapper->execute();
    return table_wrapper;
  }

  std::shared_ptr<TableWrapper> get_table_op_even_dict() {
    std::shared_ptr<Table> test_even_dict = std::make_shared<Table>(5);
    test_even_dict->add_column("a", DataType::Int);
    test_even_dict->add_column("b", DataType::Int);
    for (int i = 0; i <= 24; i += 2) test_even_dict->append({i, 100 + i});
    ChunkEncoder::encode_chunks(test_even_dict, {ChunkID{0}, ChunkID{1}}, {_encoding_type});

    auto table_wrapper_even_dict = std::make_shared<TableWrapper>(std::move(test_even_dict));
    table_wrapper_even_dict->execute();

    return table_wrapper_even_dict;
  }

  std::shared_ptr<TableWrapper> get_table_op_null() {
    auto table_wrapper_null = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_with_null.tbl", 2));
    table_wrapper_null->execute();
    return table_wrapper_null;
  }

  std::shared_ptr<TableWrapper> get_table_op_part_dict() {
    auto table = std::make_shared<Table>(5);
    table->add_column("a", DataType::Int);
    table->add_column("b", DataType::Float);

    for (int i = 1; i < 20; ++i) {
      table->append({i, 100.1 + i});
    }

    ChunkEncoder::encode_chunks(table, {ChunkID{0}, ChunkID{2}}, {_encoding_type});

    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();

    return table_wrapper;
  }

  std::shared_ptr<TableWrapper> get_table_op_filtered() {
    auto table = std::make_shared<Table>(5);
    table->add_column_definition("a", DataType::Int);
    table->add_column_definition("b", DataType::Float);

    const auto test_table_part_dict = get_table_op_part_dict()->get_output();

    auto pos_list = std::make_shared<PosList>();
    pos_list->emplace_back(RowID{ChunkID{3}, 1});
    pos_list->emplace_back(RowID{ChunkID{2}, 0});
    pos_list->emplace_back(RowID{ChunkID{1}, 1});
    pos_list->emplace_back(RowID{ChunkID{3}, 3});
    pos_list->emplace_back(RowID{ChunkID{1}, 3});
    pos_list->emplace_back(RowID{ChunkID{0}, 2});
    pos_list->emplace_back(RowID{ChunkID{2}, 2});
    pos_list->emplace_back(RowID{ChunkID{2}, 4});
    pos_list->emplace_back(RowID{ChunkID{0}, 0});
    pos_list->emplace_back(RowID{ChunkID{0}, 4});

    auto col_a = std::make_shared<ReferenceColumn>(test_table_part_dict, ColumnID{0}, pos_list);
    auto col_b = std::make_shared<ReferenceColumn>(test_table_part_dict, ColumnID{1}, pos_list);

    auto chunk = std::make_shared<Chunk>();
    chunk->add_column(col_a);
    chunk->add_column(col_b);

    table->emplace_chunk(std::move(chunk));
    auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper->execute();
    return table_wrapper;
  }

  std::shared_ptr<TableWrapper> get_table_op_with_n_dict_entries(const int num_entries) {
    // Set up dictionary encoded table with a dictionary consisting of num_entries entries.
    auto table = std::make_shared<opossum::Table>();
    table->add_column("a", DataType::Int);
    table->add_column("b", DataType::Float);

    for (int i = 0; i <= num_entries; i++) {
      table->append({i, 100.0f + i});
    }

    ChunkEncoder::encode_chunks(table, {ChunkID{0}}, {_encoding_type});

    auto table_wrapper = std::make_shared<opossum::TableWrapper>(std::move(table));
    table_wrapper->execute();
    return table_wrapper;
  }

  std::shared_ptr<const Table> to_referencing_table(const std::shared_ptr<const Table>& table) {
    auto table_out = std::make_shared<Table>();

    auto pos_list = std::make_shared<PosList>();
    pos_list->reserve(table->row_count());

    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk->size(); ++chunk_offset) {
        pos_list->push_back(RowID{chunk_id, chunk_offset});
      }
    }

    auto chunk_out = std::make_shared<Chunk>();

    for (auto column_id = ColumnID{0u}; column_id < table->column_count(); ++column_id) {
      table_out->add_column_definition(table->column_name(column_id), table->column_type(column_id));

      auto column_out = std::make_shared<ReferenceColumn>(table, column_id, pos_list);
      chunk_out->add_column(column_out);
    }

    table_out->emplace_chunk(std::move(chunk_out));
    return table_out;
  }

  std::shared_ptr<const Table> create_referencing_table_w_null_row_id(const bool references_dict_column) {
    const auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

    if (references_dict_column) {
      ChunkEncoder::encode_all_chunks(table, {_encoding_type});
    }

    auto pos_list_a = std::make_shared<PosList>(
        PosList{RowID{ChunkID{0u}, 1u}, RowID{ChunkID{1u}, 0u}, RowID{ChunkID{0u}, 2u}, RowID{ChunkID{0u}, 3u}});
    auto ref_column_a = std::make_shared<ReferenceColumn>(table, ColumnID{0u}, pos_list_a);

    auto pos_list_b = std::make_shared<PosList>(
        PosList{NULL_ROW_ID, RowID{ChunkID{0u}, 0u}, RowID{ChunkID{1u}, 2u}, RowID{ChunkID{0u}, 1u}});
    auto ref_column_b = std::make_shared<ReferenceColumn>(table, ColumnID{1u}, pos_list_b);

    auto ref_table = std::make_shared<Table>();
    ref_table->add_column_definition("a", DataType::Int, true);
    ref_table->add_column_definition("b", DataType::Float, true);

    auto chunk = std::make_shared<Chunk>();
    chunk->add_column(ref_column_a);
    chunk->add_column(ref_column_b);

    ref_table->emplace_chunk(std::move(chunk));

    return ref_table;
  }

  void scan_for_null_values(const std::shared_ptr<AbstractOperator> in,
                            const std::map<PredicateCondition, std::vector<AllTypeVariant>>& tests) {
    for (const auto& test : tests) {
      const auto predicate_condition = test.first;
      const auto& expected = test.second;

      auto scan = std::make_shared<opossum::TableScan>(in, ColumnID{1} /* "b" */, predicate_condition, NULL_VALUE);
      scan->execute();

      const auto expected_result = std::vector<AllTypeVariant>{{12, 123}};
      ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
    }
  }

  void ASSERT_COLUMN_EQ(std::shared_ptr<const Table> table, const ColumnID& column_id,
                        std::vector<AllTypeVariant> expected) {
    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk->size(); ++chunk_offset) {
        const auto& column = *chunk->get_column(column_id);

        const auto found_value = column[chunk_offset];
        const auto comparator = [found_value](const AllTypeVariant expected_value) {
          // returns equivalency, not equality to simulate std::multiset.
          // multiset cannot be used because it triggers a compiler / lib bug when built in CI
          return !(found_value < expected_value) && !(expected_value < found_value);
        };

        auto search = std::find_if(expected.begin(), expected.end(), comparator);

        ASSERT_TRUE(search != expected.end());
        expected.erase(search);
      }
    }

    ASSERT_EQ(expected.size(), 0u);
  }

 protected:
  EncodingType _encoding_type;
};

auto formatter = [](const ::testing::TestParamInfo<EncodingType> info) {
  return std::to_string(static_cast<uint32_t>(info.param));
};

// As long as two implementation of dictionary encoding exist, this ensure to run the tests for both.
INSTANTIATE_TEST_CASE_P(DictionaryEncodingTypes, OperatorsTableScanTest,
                        ::testing::Values(EncodingType::DeprecatedDictionary, EncodingType::Dictionary), formatter);

TEST_P(OperatorsTableScanTest, DoubleScan) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);

  auto scan_1 = std::make_shared<TableScan>(get_table_op(), ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  scan_1->execute();

  auto scan_2 = std::make_shared<TableScan>(scan_1, ColumnID{1}, PredicateCondition::LessThan, 457.9);
  scan_2->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan_2->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, EmptyResultScan) {
  auto scan_1 = std::make_shared<TableScan>(get_table_op(), ColumnID{0}, PredicateCondition::GreaterThan, 90000);
  scan_1->execute();

  for (auto i = ChunkID{0}; i < scan_1->get_output()->chunk_count(); i++)
    EXPECT_EQ(scan_1->get_output()->get_chunk(i)->column_count(), 2u);
}

TEST_P(OperatorsTableScanTest, SingleScanReturnsCorrectRowCount) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);

  auto scan = std::make_shared<TableScan>(get_table_op(), ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  scan->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, ScanOnDictColumn) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {104};
  tests[PredicateCondition::NotEquals] = {100, 102, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[PredicateCondition::LessThan] = {100, 102};
  tests[PredicateCondition::LessThanEquals] = {100, 102, 104};
  tests[PredicateCondition::GreaterThan] = {106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[PredicateCondition::GreaterThanEquals] = {104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[PredicateCondition::Between] = {};  // Will throw
  tests[PredicateCondition::IsNull] = {};
  tests[PredicateCondition::IsNotNull] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};

  for (const auto& test : tests) {
    auto scan = std::make_shared<TableScan>(get_table_op_even_dict(), ColumnID{0}, test.first, 4);

    if (test.first == PredicateCondition::Between) {
      EXPECT_THROW(scan->execute(), std::logic_error);
      continue;
    }

    scan->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanOnReferencedDictColumn) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {104};
  tests[PredicateCondition::NotEquals] = {100, 102, 106};
  tests[PredicateCondition::LessThan] = {100, 102};
  tests[PredicateCondition::LessThanEquals] = {100, 102, 104};
  tests[PredicateCondition::GreaterThan] = {106};
  tests[PredicateCondition::GreaterThanEquals] = {104, 106};
  tests[PredicateCondition::Between] = {};  // Will throw
  tests[PredicateCondition::IsNull] = {};
  tests[PredicateCondition::IsNotNull] = {100, 102, 104, 106};

  for (const auto& test : tests) {
    auto scan1 = std::make_shared<TableScan>(get_table_op_even_dict(), ColumnID{1}, PredicateCondition::LessThan, 108);
    scan1->execute();

    auto scan2 = std::make_shared<TableScan>(scan1, ColumnID{0}, test.first, 4);

    if (test.first == PredicateCondition::Between) {
      EXPECT_THROW(scan2->execute(), std::logic_error);
      continue;
    }

    scan2->execute();

    ASSERT_COLUMN_EQ(scan2->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanPartiallyCompressed) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_seq_filtered.tbl", 2);

  auto table_wrapper = get_table_op_part_dict();
  auto scan_1 = std::make_shared<TableScan>(table_wrapper, ColumnID{0}, PredicateCondition::LessThan, 10);
  scan_1->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan_1->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, ScanWeirdPosList) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_seq_filtered_onlyodd.tbl", 2);

  auto table_wrapper = get_table_op_filtered();
  auto scan_1 = std::make_shared<TableScan>(table_wrapper, ColumnID{0}, PredicateCondition::LessThan, 10);
  scan_1->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan_1->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, ScanOnDictColumnValueGreaterThanMaxDictionaryValue) {
  const auto all_rows = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  const auto no_rows = std::vector<AllTypeVariant>{};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;
  tests[PredicateCondition::LessThan] = all_rows;
  tests[PredicateCondition::LessThanEquals] = all_rows;
  tests[PredicateCondition::GreaterThan] = no_rows;
  tests[PredicateCondition::GreaterThanEquals] = no_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<TableScan>(get_table_op_even_dict(), ColumnID{0}, test.first, 30);
    scan->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanOnDictColumnValueLessThanMinDictionaryValue) {
  const auto all_rows = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  const auto no_rows = std::vector<AllTypeVariant>{};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;
  tests[PredicateCondition::LessThan] = no_rows;
  tests[PredicateCondition::LessThanEquals] = no_rows;
  tests[PredicateCondition::GreaterThan] = all_rows;
  tests[PredicateCondition::GreaterThanEquals] = all_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<TableScan>(get_table_op_even_dict(), ColumnID{0} /* "a" */, test.first, -10);
    scan->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanOnIntValueColumnWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, PredicateCondition::GreaterThan,
                                          ColumnID{1} /* "b" */);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnReferencedIntValueColumnWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, PredicateCondition::GreaterThan,
                                          ColumnID{1} /* "b" */);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnIntDictColumnWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, {_encoding_type});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, PredicateCondition::GreaterThan,
                                          ColumnID{1} /* "b" */);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnReferencedIntDictColumnWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, {_encoding_type});

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, PredicateCondition::GreaterThan,
                                          ColumnID{1} /* "b" */);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnDictColumnAroundBounds) {
  // scanning for a value that is around the dictionary's bounds

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {100};
  tests[PredicateCondition::LessThan] = {};
  tests[PredicateCondition::LessThanEquals] = {100};
  tests[PredicateCondition::GreaterThan] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[PredicateCondition::GreaterThanEquals] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[PredicateCondition::NotEquals] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[PredicateCondition::IsNull] = {};
  tests[PredicateCondition::IsNotNull] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};

  for (const auto& test : tests) {
    auto scan = std::make_shared<opossum::TableScan>(get_table_op_even_dict(), ColumnID{0}, test.first, 0);
    scan->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanWithEmptyInput) {
  auto scan_1 =
      std::make_shared<opossum::TableScan>(get_table_op(), ColumnID{0}, PredicateCondition::GreaterThan, 12345);
  scan_1->execute();
  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(0));

  // scan_1 produced an empty result
  auto scan_2 = std::make_shared<opossum::TableScan>(scan_1, ColumnID{1}, PredicateCondition::Equals, 456.7);
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(0));
}

TEST_P(OperatorsTableScanTest, ScanOnWideDictionaryColumn) {
  // 2**8 + 1 values require a data type of 16bit.
  const auto table_wrapper_dict_16 = get_table_op_with_n_dict_entries((1 << 8) + 1);
  auto scan_1 =
      std::make_shared<opossum::TableScan>(table_wrapper_dict_16, ColumnID{0}, PredicateCondition::GreaterThan, 200);
  scan_1->execute();

  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(57));

  // 2**16 + 1 values require a data type of 32bit.
  const auto table_wrapper_dict_32 = get_table_op_with_n_dict_entries((1 << 16) + 1);
  auto scan_2 =
      std::make_shared<opossum::TableScan>(table_wrapper_dict_32, ColumnID{0}, PredicateCondition::GreaterThan, 65500);
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(37));
}

TEST_P(OperatorsTableScanTest, OperatorName) {
  auto scan_1 =
      std::make_shared<opossum::TableScan>(get_table_op(), ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);

  EXPECT_EQ(scan_1->name(), "TableScan");
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnValueColumn) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnDictColumn) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, {_encoding_type});

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnValueColumnWithoutNulls) {
  auto table = load_table("src/test/tables/int_float.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {}}, {PredicateCondition::IsNotNull, {12345, 123, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnReferencedValueColumnWithoutNulls) {
  auto table = load_table("src/test/tables/int_float.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {}}, {PredicateCondition::IsNotNull, {12345, 123, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnReferencedValueColumn) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnReferencedDictColumn) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, {_encoding_type});

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesWithNullRowIDOnReferencedValueColumn) {
  auto table = create_referencing_table_w_null_row_id(false);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {123, 1234}}, {PredicateCondition::IsNotNull, {12345, NULL_VALUE}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesWithNullRowIDOnReferencedDictColumn) {
  auto table = create_referencing_table_w_null_row_id(true);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {123, 1234}}, {PredicateCondition::IsNotNull, {12345, NULL_VALUE}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, NullSemantics) {
  const auto predicate_conditions = std::vector<PredicateCondition>(
      {PredicateCondition::Equals, PredicateCondition::NotEquals, PredicateCondition::LessThan,
       PredicateCondition::LessThanEquals, PredicateCondition::GreaterThan, PredicateCondition::GreaterThanEquals});

  for (auto predicate_condition : predicate_conditions) {
    auto scan = std::make_shared<TableScan>(get_table_op_null(), ColumnID{0}, predicate_condition, NULL_VALUE);
    scan->execute();

    EXPECT_EQ(scan->get_output()->row_count(), 0u);

    for (auto i = ChunkID{0}; i < scan->get_output()->chunk_count(); i++) {
      EXPECT_EQ(scan->get_output()->get_chunk(i)->column_count(), 2u);
    }
  }
}

TEST_P(OperatorsTableScanTest, ScanWithExcludedFirstChunk) {
  const auto expected = std::vector<AllTypeVariant>{110, 112, 114, 116, 118, 120, 122, 124};

  auto scan = std::make_shared<opossum::TableScan>(get_table_op_even_dict(), ColumnID{0},
                                                   PredicateCondition::GreaterThanEquals, 0);
  scan->set_excluded_chunk_ids({ChunkID{0u}});
  scan->execute();

  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, expected);
}

}  // namespace opossum
