#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/table_wrapper.hpp"
#include "../../lib/storage/dictionary_compression.hpp"
#include "../../lib/storage/reference_column.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsTableScanTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper->execute();

    std::shared_ptr<Table> test_even_dict = std::make_shared<Table>(5);
    test_even_dict->add_column("a", "int");
    test_even_dict->add_column("b", "int");
    for (int i = 0; i <= 24; i += 2) test_even_dict->append({i, 100 + i});
    DictionaryCompression::compress_chunks(*test_even_dict, {ChunkID{0}, ChunkID{1}});

    _table_wrapper_even_dict = std::make_shared<TableWrapper>(std::move(test_even_dict));
    _table_wrapper_even_dict->execute();
  }

  std::shared_ptr<TableWrapper> get_table_op_part_dict() {
    auto table = std::make_shared<Table>(5);
    table->add_column("a", "int");
    table->add_column("b", "float");

    for (int i = 1; i < 20; ++i) {
      table->append({i, 100.1 + i});
    }

    DictionaryCompression::compress_chunks(*table, {ChunkID{0}, ChunkID{2}});

    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();

    return table_wrapper;
  }

  std::shared_ptr<TableWrapper> get_table_op_filtered() {
    auto table = std::make_shared<Table>(5);
    table->add_column_definition("a", "int");
    table->add_column_definition("b", "float");

    const auto test_table_part_dict = get_table_op_part_dict()->get_output();

    auto pos_list = std::make_shared<PosList>();
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{3}, 1));
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{2}, 0));
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{1}, 1));
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{3}, 3));
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{1}, 3));
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{0}, 2));
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{2}, 2));
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{2}, 4));
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{0}, 0));
    pos_list->emplace_back(test_table_part_dict->calculate_row_id(ChunkID{0}, 4));

    auto col_a = std::make_shared<ReferenceColumn>(test_table_part_dict, ColumnID{0}, pos_list);
    auto col_b = std::make_shared<ReferenceColumn>(test_table_part_dict, ColumnID{1}, pos_list);

    Chunk chunk;
    chunk.add_column(col_a);
    chunk.add_column(col_b);

    table->add_chunk(std::move(chunk));
    auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper->execute();
    return table_wrapper;
  }

  std::shared_ptr<TableWrapper> get_table_op_with_n_dict_entries(const int num_entries) {
    // Set up dictionary encoded table with a dictionary consisting of num_entries entries.
    auto table = std::make_shared<opossum::Table>(0);
    table->add_column("a", "int");
    table->add_column("b", "float");

    for (int i = 0; i <= num_entries; i++) {
      table->append({i, 100.0f + i});
    }

    DictionaryCompression::compress_chunks(*table, {ChunkID{0}});

    auto table_wrapper = std::make_shared<opossum::TableWrapper>(std::move(table));
    table_wrapper->execute();
    return table_wrapper;
  }

  std::shared_ptr<const Table> to_referencing_table(const std::shared_ptr<const Table>& table) {
    auto table_out = std::make_shared<Table>();

    auto pos_list = std::make_shared<PosList>();
    pos_list->reserve(table->row_count());

    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk.size(); ++chunk_offset) {
        pos_list->push_back(RowID{chunk_id, chunk_offset});
      }
    }

    auto chunk_out = Chunk{};

    for (auto column_id = ColumnID{0u}; column_id < table->col_count(); ++column_id) {
      table_out->add_column_definition(table->column_name(column_id), table->column_type(column_id));

      auto column_out = std::make_shared<ReferenceColumn>(table, column_id, pos_list);
      chunk_out.add_column(column_out);
    }

    table_out->add_chunk(std::move(chunk_out));
    return table_out;
  }

  std::shared_ptr<const Table> create_referencing_table_w_null_row_id(const bool references_dict_column) {
    const auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

    if (references_dict_column) {
      DictionaryCompression::compress_table(*table);
    }

    auto pos_list_a = std::make_shared<PosList>(
        PosList{RowID{ChunkID{0u}, 1u}, RowID{ChunkID{1u}, 0u}, RowID{ChunkID{0u}, 2u}, RowID{ChunkID{0u}, 3u}});
    auto ref_column_a = std::make_shared<ReferenceColumn>(table, ColumnID{0u}, pos_list_a);

    auto pos_list_b = std::make_shared<PosList>(
        PosList{NULL_ROW_ID, RowID{ChunkID{0u}, 0u}, RowID{ChunkID{1u}, 2u}, RowID{ChunkID{0u}, 1u}});
    auto ref_column_b = std::make_shared<ReferenceColumn>(table, ColumnID{1u}, pos_list_b);

    auto ref_table = std::make_shared<Table>();
    ref_table->add_column_definition("a", "int", true);
    ref_table->add_column_definition("b", "float", true);

    auto chunk = Chunk{};
    chunk.add_column(ref_column_a);
    chunk.add_column(ref_column_b);

    ref_table->add_chunk(std::move(chunk));

    return ref_table;
  }

  void scan_for_null_values(const std::shared_ptr<AbstractOperator> in,
                            const std::map<ScanType, std::vector<AllTypeVariant>>& tests) {
    for (const auto& test : tests) {
      const auto scan_type = test.first;
      const auto& expected = test.second;

      auto scan = std::make_shared<opossum::TableScan>(in, ColumnID{1} /* "b" */, scan_type, NULL_VALUE);
      scan->execute();

      const auto expected_result = std::vector<AllTypeVariant>{{12, 123}};
      ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
    }
  }

  void ASSERT_COLUMN_EQ(std::shared_ptr<const Table> table, const ColumnID& column_id,
                        std::vector<AllTypeVariant> expected) {
    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk.size(); ++chunk_offset) {
        const auto& column = *chunk.get_column(column_id);

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

  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_even_dict;
};

TEST_F(OperatorsTableScanTest, DoubleScan) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);

  auto scan_1 = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);
  scan_1->execute();

  auto scan_2 = std::make_shared<TableScan>(scan_1, ColumnID{1}, ScanType::OpLessThan, 457.9);
  scan_2->execute();

  EXPECT_TABLE_EQ(scan_2->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, EmptyResultScan) {
  auto scan_1 = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThan, 90000);
  scan_1->execute();

  for (auto i = ChunkID{0}; i < scan_1->get_output()->chunk_count(); i++)
    EXPECT_EQ(scan_1->get_output()->get_chunk(i).col_count(), 2u);
}

TEST_F(OperatorsTableScanTest, SingleScanReturnsCorrectRowCount) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);

  auto scan = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);
  scan->execute();

  EXPECT_TABLE_EQ(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumn) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  std::map<ScanType, std::vector<AllTypeVariant>> tests;
  tests[ScanType::OpEquals] = {104};
  tests[ScanType::OpNotEquals] = {100, 102, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpLessThan] = {100, 102};
  tests[ScanType::OpLessThanEquals] = {100, 102, 104};
  tests[ScanType::OpGreaterThan] = {106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpGreaterThanEquals] = {104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpBetween] = {104, 106, 108};
  for (const auto& test : tests) {
    auto scan =
        std::make_shared<TableScan>(_table_wrapper_even_dict, ColumnID{0}, test.first, 4, optional<AllTypeVariant>(9));
    scan->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TEST_F(OperatorsTableScanTest, ScanOnReferencedDictColumn) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  std::map<ScanType, std::vector<AllTypeVariant>> tests;
  tests[ScanType::OpEquals] = {104};
  tests[ScanType::OpNotEquals] = {100, 102, 106};
  tests[ScanType::OpLessThan] = {100, 102};
  tests[ScanType::OpLessThanEquals] = {100, 102, 104};
  tests[ScanType::OpGreaterThan] = {106};
  tests[ScanType::OpGreaterThanEquals] = {104, 106};
  tests[ScanType::OpBetween] = {104, 106};
  for (const auto& test : tests) {
    auto scan1 = std::make_shared<TableScan>(_table_wrapper_even_dict, ColumnID{1}, ScanType::OpLessThan, 108);
    scan1->execute();

    auto scan2 = std::make_shared<TableScan>(scan1, ColumnID{0}, test.first, 4, optional<AllTypeVariant>(9));
    scan2->execute();

    ASSERT_COLUMN_EQ(scan2->get_output(), ColumnID{1}, test.second);
  }
}

TEST_F(OperatorsTableScanTest, ScanPartiallyCompressed) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_seq_filtered.tbl", 2);

  auto table_wrapper = get_table_op_part_dict();
  auto scan_1 = std::make_shared<TableScan>(table_wrapper, ColumnID{0}, ScanType::OpLessThan, 10);
  scan_1->execute();

  EXPECT_TABLE_EQ(scan_1->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanWeirdPosList) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_seq_filtered_onlyodd.tbl", 2);

  auto table_wrapper = get_table_op_filtered();
  auto scan_1 = std::make_shared<TableScan>(table_wrapper, ColumnID{0}, ScanType::OpLessThan, 10);
  scan_1->execute();

  EXPECT_TABLE_EQ(scan_1->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumnValueGreaterThanMaxDictionaryValue) {
  const auto all_rows = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  const auto no_rows = std::vector<AllTypeVariant>{};

  std::map<ScanType, std::vector<AllTypeVariant>> tests;
  tests[ScanType::OpEquals] = no_rows;
  tests[ScanType::OpNotEquals] = all_rows;
  tests[ScanType::OpLessThan] = all_rows;
  tests[ScanType::OpLessThanEquals] = all_rows;
  tests[ScanType::OpGreaterThan] = no_rows;
  tests[ScanType::OpGreaterThanEquals] = no_rows;
  tests[ScanType::OpBetween] = no_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<TableScan>(_table_wrapper_even_dict, ColumnID{0}, test.first, 30,
                                            optional<AllTypeVariant>(34));
    scan->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumnValueLessThanMinDictionaryValue) {
  const auto all_rows = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  const auto no_rows = std::vector<AllTypeVariant>{};

  std::map<ScanType, std::vector<AllTypeVariant>> tests;
  tests[ScanType::OpEquals] = no_rows;
  tests[ScanType::OpNotEquals] = all_rows;
  tests[ScanType::OpLessThan] = no_rows;
  tests[ScanType::OpLessThanEquals] = no_rows;
  tests[ScanType::OpGreaterThan] = all_rows;
  tests[ScanType::OpGreaterThanEquals] = all_rows;
  tests[ScanType::OpBetween] = all_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<TableScan>(_table_wrapper_even_dict, ColumnID{0} /* "a" */, test.first, -10,
                                            optional<AllTypeVariant>(34));
    scan->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TEST_F(OperatorsTableScanTest, ScanOnIntValueColumnWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan =
      std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, ScanType::OpGreaterThan, ColumnID{1} /* "b" */);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_F(OperatorsTableScanTest, ScanOnReferencedIntValueColumnWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  auto scan =
      std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, ScanType::OpGreaterThan, ColumnID{1} /* "b" */);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_F(OperatorsTableScanTest, ScanOnIntDictColumnWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  DictionaryCompression::compress_table(*table);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan =
      std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, ScanType::OpGreaterThan, ColumnID{1} /* "b" */);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_F(OperatorsTableScanTest, ScanOnReferencedIntDictColumnWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  DictionaryCompression::compress_table(*table);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  auto scan =
      std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, ScanType::OpGreaterThan, ColumnID{1} /* "b" */);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumnAroundBounds) {
  // scanning for a value that is around the dictionary's bounds

  std::map<ScanType, std::vector<AllTypeVariant>> tests;
  tests[ScanType::OpEquals] = {100};
  tests[ScanType::OpLessThan] = {};
  tests[ScanType::OpLessThanEquals] = {100};
  tests[ScanType::OpGreaterThan] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpGreaterThanEquals] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpNotEquals] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpBetween] = {100, 102, 104, 106, 108, 110};

  for (const auto& test : tests) {
    auto scan = std::make_shared<opossum::TableScan>(_table_wrapper_even_dict, ColumnID{0}, test.first, 0,
                                                     optional<AllTypeVariant>(10));
    scan->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TEST_F(OperatorsTableScanTest, ScanWithEmptyInput) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThan, 12345);
  scan_1->execute();
  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(0));

  // scan_1 produced an empty result
  auto scan_2 = std::make_shared<opossum::TableScan>(scan_1, ColumnID{1}, ScanType::OpEquals, 456.7);
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(0));
}

TEST_F(OperatorsTableScanTest, ScanOnWideDictionaryColumn) {
  // 2**8 + 1 values require a data type of 16bit.
  const auto table_wrapper_dict_16 = get_table_op_with_n_dict_entries((1 << 8) + 1);
  auto scan_1 = std::make_shared<opossum::TableScan>(table_wrapper_dict_16, ColumnID{0}, ScanType::OpGreaterThan, 200);
  scan_1->execute();

  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(57));

  // 2**16 + 1 values require a data type of 32bit.
  const auto table_wrapper_dict_32 = get_table_op_with_n_dict_entries((1 << 16) + 1);
  auto scan_2 =
      std::make_shared<opossum::TableScan>(table_wrapper_dict_32, ColumnID{0}, ScanType::OpGreaterThan, 65500);
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(37));
}

TEST_F(OperatorsTableScanTest, NumInputTables) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);
  scan_1->execute();

  EXPECT_EQ(scan_1->num_in_tables(), 1);
}

TEST_F(OperatorsTableScanTest, NumOutputTables) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);

  EXPECT_EQ(scan_1->num_out_tables(), 1);
}

TEST_F(OperatorsTableScanTest, OperatorName) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);

  EXPECT_EQ(scan_1->name(), "TableScan");
}

TEST_F(OperatorsTableScanTest, ScanForNullValuesOnValueColumn) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4));
  table_wrapper->execute();

  const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{
      {ScanType::OpEquals, {12, 123}}, {ScanType::OpNotEquals, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_F(OperatorsTableScanTest, ScanForNullValuesOnDictColumn) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  DictionaryCompression::compress_table(*table);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{
      {ScanType::OpEquals, {12, 123}}, {ScanType::OpNotEquals, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_F(OperatorsTableScanTest, ScanForNullValuesOnReferencedValueColumn) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{
      {ScanType::OpEquals, {12, 123}}, {ScanType::OpNotEquals, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_F(OperatorsTableScanTest, ScanForNullValuesOnReferencedDictColumn) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  DictionaryCompression::compress_table(*table);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{
      {ScanType::OpEquals, {12, 123}}, {ScanType::OpNotEquals, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_F(OperatorsTableScanTest, ScanForNullValuesWithNullRowIDOnReferencedValueColumn) {
  auto table = create_referencing_table_w_null_row_id(false);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{{ScanType::OpEquals, {123, 1234}},
                                                                     {ScanType::OpNotEquals, {12345, NULL_VALUE}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_F(OperatorsTableScanTest, ScanForNullValuesWithNullRowIDOnReferencedDictColumn) {
  auto table = create_referencing_table_w_null_row_id(true);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{{ScanType::OpEquals, {123, 1234}},
                                                                     {ScanType::OpNotEquals, {12345, NULL_VALUE}}};

  scan_for_null_values(table_wrapper, tests);
}

}  // namespace opossum
