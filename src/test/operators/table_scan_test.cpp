#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/table_wrapper.hpp"
#include "../../lib/storage/dictionary_compression.hpp"
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
    for (auto i = 0u; i <= 24u; i += 2) test_even_dict->append({i, 100 + i});
    DictionaryCompression::compress_chunks(*test_even_dict, {ChunkID{0}, ChunkID{1}});

    _table_wrapper_even_dict = std::make_shared<TableWrapper>(std::move(test_even_dict));
    _table_wrapper_even_dict->execute();
  }

  std::shared_ptr<TableWrapper> get_table_op_part_dict() {
    auto table = std::make_shared<Table>(5);
    table->add_column("a", "int");
    table->add_column("b", "float");

    for (auto i = 1u; i < 20; ++i) {
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

  std::shared_ptr<TableWrapper> get_table_op_with_n_dict_entries(const uint32_t num_entries) {
    // Set up dictionary encoded table with a dictionary consisting of num_entries entries.
    std::shared_ptr<TableWrapper> table_wrapper;
    auto table = std::make_shared<opossum::Table>(0);
    table->add_column("a", "int");
    table->add_column("b", "float");
    for (auto i = 0u; i <= num_entries; i += 1) table->append({i, 100.0f + i});
    DictionaryCompression::compress_chunks(*table, {ChunkID{0}});

    table_wrapper = std::make_shared<opossum::TableWrapper>(std::move(table));
    table_wrapper->execute();
    return table_wrapper;
  }

  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_even_dict;
};

TEST_F(OperatorsTableScanTest, DoubleScan) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);

  auto scan_1 = std::make_shared<TableScan>(_table_wrapper, ColumnName("a"), ScanType::OpGreaterThanEquals, 1234);
  scan_1->execute();

  auto scan_2 = std::make_shared<TableScan>(scan_1, ColumnName("b"), ScanType::OpLessThan, 457.9);
  scan_2->execute();

  EXPECT_TABLE_EQ(scan_2->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, EmptyResultScan) {
  auto scan_1 = std::make_shared<TableScan>(_table_wrapper, "a", ScanType::OpGreaterThan, 90000);
  scan_1->execute();

  for (auto i = ChunkID{0}; i < scan_1->get_output()->chunk_count(); i++)
    EXPECT_EQ(scan_1->get_output()->get_chunk(i).col_count(), 2u);
}

TEST_F(OperatorsTableScanTest, SingleScanReturnsCorrectRowCount) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);

  auto scan = std::make_shared<TableScan>(_table_wrapper, ColumnName("a"), ScanType::OpGreaterThanEquals, 1234);
  scan->execute();

  EXPECT_TABLE_EQ(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumn) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  std::map<ScanType, std::set<int>> tests;
  tests[ScanType::OpEquals] = {104};
  tests[ScanType::OpNotEquals] = {100, 102, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpLessThan] = {100, 102};
  tests[ScanType::OpLessThanEquals] = {100, 102, 104};
  tests[ScanType::OpGreaterThan] = {106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpGreaterThanEquals] = {104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpBetween] = {104, 106, 108};
  for (const auto& test : tests) {
    auto scan = std::make_shared<TableScan>(_table_wrapper_even_dict, ColumnName("a"), test.first, 4,
                                            optional<AllTypeVariant>(9));
    scan->execute();

    auto expected_copy = test.second;
    for (ChunkID chunk_id{0}; chunk_id < scan->get_output()->chunk_count(); ++chunk_id) {
      auto& chunk = scan->get_output()->get_chunk(chunk_id);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        EXPECT_EQ(expected_copy.erase(type_cast<int>((*chunk.get_column(ColumnID{1}))[chunk_offset])), 1ull);
      }
    }
    EXPECT_EQ(expected_copy.size(), 0ull);
  }
}

TEST_F(OperatorsTableScanTest, ScanOnReferencedDictColumn) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  std::map<ScanType, std::set<int>> tests;
  tests[ScanType::OpEquals] = {104};
  tests[ScanType::OpNotEquals] = {100, 102, 106};
  tests[ScanType::OpLessThan] = {100, 102};
  tests[ScanType::OpLessThanEquals] = {100, 102, 104};
  tests[ScanType::OpGreaterThan] = {106};
  tests[ScanType::OpGreaterThanEquals] = {104, 106};
  tests[ScanType::OpBetween] = {104, 106};
  for (const auto& test : tests) {
    auto scan1 = std::make_shared<TableScan>(_table_wrapper_even_dict, ColumnName("b"), ScanType::OpLessThan, 108);
    scan1->execute();

    auto scan2 = std::make_shared<TableScan>(scan1, ColumnName("a"), test.first, 4, optional<AllTypeVariant>(9));
    scan2->execute();

    auto expected_copy = test.second;
    for (ChunkID chunk_id{0}; chunk_id < scan2->get_output()->chunk_count(); ++chunk_id) {
      auto& chunk = scan2->get_output()->get_chunk(chunk_id);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        EXPECT_EQ(expected_copy.erase(type_cast<int>((*chunk.get_column(ColumnID{1}))[chunk_offset])), 1ull);
      }
    }
    EXPECT_EQ(expected_copy.size(), (size_t)0);
  }
}

TEST_F(OperatorsTableScanTest, ScanPartiallyCompressed) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_seq_filtered.tbl", 2);

  auto table_wrapper = get_table_op_part_dict();
  auto scan_1 = std::make_shared<TableScan>(table_wrapper, ColumnName("a"), ScanType::OpLessThan, 10);
  scan_1->execute();

  EXPECT_TABLE_EQ(scan_1->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanWeirdPosList) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_seq_filtered_onlyodd.tbl", 2);

  auto table_wrapper = get_table_op_filtered();
  auto scan_1 = std::make_shared<TableScan>(table_wrapper, ColumnName("a"), ScanType::OpLessThan, 10);
  scan_1->execute();

  EXPECT_TABLE_EQ(scan_1->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumnValueGreaterMaxDictionaryValue) {
  // We compare column values with 30 which is greater than the greatest dictionary entry.
  std::map<ScanType, std::set<int>> tests;
  tests[ScanType::OpEquals] = {};
  tests[ScanType::OpNotEquals] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpLessThan] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpLessThanEquals] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpGreaterThan] = {};
  tests[ScanType::OpGreaterThanEquals] = {};
  tests[ScanType::OpBetween] = {};
  for (const auto& test : tests) {
    auto scan = std::make_shared<TableScan>(_table_wrapper_even_dict, ColumnName("a"), test.first, 30,
                                            optional<AllTypeVariant>(34));
    scan->execute();

    auto expected_copy = test.second;
    for (ChunkID chunk_id{0}; chunk_id < scan->get_output()->chunk_count(); ++chunk_id) {
      auto& chunk = scan->get_output()->get_chunk(chunk_id);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        EXPECT_EQ(expected_copy.erase(type_cast<int>((*chunk.get_column(ColumnID{1}))[chunk_offset])), 1ull);
      }
    }
    EXPECT_EQ(expected_copy.size(), 0ull);
  }
}

TEST_F(OperatorsTableScanTest, ScanWithColumn) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int_int.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_column_parameter.tbl", 1);

  auto scan = std::make_shared<TableScan>(table_wrapper, ColumnName("b"), ScanType::OpEquals, ColumnName("a"));
  scan->execute();
  EXPECT_TABLE_EQ(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanOnDictWithColumn) {
  auto table = load_table("src/test/tables/int_int_int.tbl", 2);
  DictionaryCompression::compress_chunks(*table, {ChunkID{0}, ChunkID{1}});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  const auto expected_result = load_table("src/test/tables/int_int_int_column_parameter.tbl", 1);
  auto scan = std::make_shared<TableScan>(table_wrapper, ColumnName("b"), ScanType::OpEquals, ColumnName("a"));
  scan->execute();
  EXPECT_TABLE_EQ(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumnAroundBounds) {
  // scanning for a value that is around the dictionary's bounds

  std::map<ScanType, std::set<int>> tests;
  tests[ScanType::OpEquals] = {100};
  tests[ScanType::OpLessThan] = {};
  tests[ScanType::OpLessThanEquals] = {100};
  tests[ScanType::OpGreaterThan] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpGreaterThanEquals] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpNotEquals] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpBetween] = {100, 102, 104, 106, 108, 110};

  for (const auto& test : tests) {
    auto scan = std::make_shared<opossum::TableScan>(_table_wrapper_even_dict, "a", test.first, 0,
                                                     optional<AllTypeVariant>(10));
    scan->execute();

    auto expected_copy = test.second;
    for (ChunkID chunk_id{0}; chunk_id < scan->get_output()->chunk_count(); ++chunk_id) {
      auto& chunk = scan->get_output()->get_chunk(chunk_id);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        EXPECT_EQ(expected_copy.erase(type_cast<int>((*chunk.get_column(ColumnID{1}))[chunk_offset])),
                  static_cast<size_t>(1));
      }
    }
    EXPECT_EQ(expected_copy.size(), 0ull);
  }
}

TEST_F(OperatorsTableScanTest, ScanWithEmptyInput) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ScanType::OpGreaterThan, 12345);
  scan_1->execute();
  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(0));

  // scan_1 produced an empty result
  auto scan_2 = std::make_shared<opossum::TableScan>(scan_1, "b", ScanType::OpEquals, 456.7);
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(0));
}

TEST_F(OperatorsTableScanTest, ScanOnWideDictionaryColumn) {
  // 2**8 + 1 values require a data type of 16bit.
  const auto table_wrapper_dict_16 = get_table_op_with_n_dict_entries((1 << 8) + 1);
  auto scan_1 = std::make_shared<opossum::TableScan>(table_wrapper_dict_16, "a", ScanType::OpGreaterThan, 200);
  scan_1->execute();

  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(57));

  // 2**16 + 1 values require a data type of 32bit.
  const auto table_wrapper_dict_32 = get_table_op_with_n_dict_entries((1 << 16) + 1);
  auto scan_2 = std::make_shared<opossum::TableScan>(table_wrapper_dict_32, "a", ScanType::OpGreaterThan, 65500);
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(37));
}

TEST_F(OperatorsTableScanTest, NumInputTables) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ScanType::OpGreaterThanEquals, 1234);
  scan_1->execute();

  EXPECT_EQ(scan_1->num_in_tables(), 1);
}

TEST_F(OperatorsTableScanTest, NumOutputTables) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ScanType::OpGreaterThanEquals, 1234);

  EXPECT_EQ(scan_1->num_out_tables(), 1);
}

TEST_F(OperatorsTableScanTest, OperatorName) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ScanType::OpGreaterThanEquals, 1234);

  EXPECT_EQ(scan_1->name(), "TableScan");
}

}  // namespace opossum
