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
    for (int i = 0; i <= 24; i += 2) test_even_dict->append({i, 100 + i});
    DictionaryCompression::compress_chunks(*test_even_dict, {ChunkID{0}, ChunkID{1}});

    _table_wrapper_even_dict = std::make_shared<TableWrapper>(std::move(test_even_dict));
    _table_wrapper_even_dict->execute();

    std::shared_ptr<Table> test_table_part_dict = std::make_shared<Table>(5);
    test_table_part_dict->add_column("a", "int");
    test_table_part_dict->add_column("b", "float");
    for (int i = 1; i < 20; ++i) test_table_part_dict->append({i, 100.1 + i});
    DictionaryCompression::compress_chunks(*test_table_part_dict, {ChunkID{0}, ChunkID{2}});

    _table_wrapper_part_dict = std::make_shared<TableWrapper>(test_table_part_dict);
    _table_wrapper_part_dict->execute();

    std::shared_ptr<Table> test_table_filtered = std::make_shared<Table>(5);
    test_table_filtered->add_column("a", "int", false);
    test_table_filtered->add_column("b", "float", false);
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
    test_table_filtered->add_chunk(std::move(chunk));
    _table_wrapper_filtered = std::make_shared<TableWrapper>(std::move(test_table_filtered));
    _table_wrapper_filtered->execute();

    _table_wrapper_int3 = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int_int.tbl", 2));
    _table_wrapper_int3->execute();

    std::shared_ptr<Table> test_table_int3_dict = load_table("src/test/tables/int_int_int.tbl", 2);
    DictionaryCompression::compress_chunks(*test_table_int3_dict, {ChunkID{0}, ChunkID{1}});

    _table_wrapper_int3_dict = std::make_shared<TableWrapper>(std::move(test_table_int3_dict));
    _table_wrapper_int3_dict->execute();

    // Set up dictionary encoded table with a dictionary width of 16 bit
    auto _test_table_dict_16 = std::make_shared<opossum::Table>(0);
    _test_table_dict_16->add_column("a", "int");
    _test_table_dict_16->add_column("b", "float");
    for (int i = 0; i <= 257; i += 1) _test_table_dict_16->append({i, 100.0f + i});
    DictionaryCompression::compress_chunks(*_test_table_dict_16, {ChunkID{0}});

    _table_wrapper_dict_16 = std::make_shared<opossum::TableWrapper>(std::move(_test_table_dict_16));
    _table_wrapper_dict_16->execute();

    // Set up dictionary encoded table with a dictionary width of 32 bit
    auto _test_table_dict_32 = std::make_shared<opossum::Table>(0);
    _test_table_dict_32->add_column("a", "int");
    _test_table_dict_32->add_column("b", "float");
    for (int i = 0; i <= 65537; i += 1) _test_table_dict_32->append({i, 100.0f + i});
    DictionaryCompression::compress_chunks(*_test_table_dict_32, {ChunkID{0}});

    _table_wrapper_dict_32 = std::make_shared<opossum::TableWrapper>(std::move(_test_table_dict_32));
    _table_wrapper_dict_32->execute();

    // load string table
    _table_wrapper_string = std::make_shared<TableWrapper>(load_table("src/test/tables/int_string_like.tbl", 2));
    _table_wrapper_string->execute();

    // load and compress string table
    std::shared_ptr<Table> test_table_string_dict = load_table("src/test/tables/int_string_like.tbl", 5);

    DictionaryCompression::compress_chunks(*test_table_string_dict, {ChunkID{0}});

    _table_wrapper_string_dict = std::make_shared<TableWrapper>(std::move(test_table_string_dict));
    _table_wrapper_string_dict->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_even_dict, _table_wrapper_part_dict,
      _table_wrapper_filtered, _table_wrapper_dict_16, _table_wrapper_dict_32, _table_wrapper_int3,
      _table_wrapper_int3_dict, _table_wrapper_string, _table_wrapper_string_dict;
};

TEST_F(OperatorsTableScanTest, DoubleScan) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);

  auto scan_1 = std::make_shared<TableScan>(_table_wrapper, ColumnName("a"), ">=", 1234);
  scan_1->execute();

  auto scan_2 = std::make_shared<TableScan>(scan_1, ColumnName("b"), "<", 457.9);
  scan_2->execute();

  EXPECT_TABLE_EQ(scan_2->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, EmptyResultScan) {
  auto scan_1 = std::make_shared<TableScan>(_table_wrapper, "a", ">", 90000);
  scan_1->execute();

  for (auto i = ChunkID{0}; i < scan_1->get_output()->chunk_count(); i++)
    EXPECT_EQ(scan_1->get_output()->get_chunk(i).col_count(), 2u);
}

TEST_F(OperatorsTableScanTest, SingleScanReturnsCorrectRowCount) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);

  auto scan = std::make_shared<TableScan>(_table_wrapper, ColumnName("a"), ">=", 1234);
  scan->execute();

  EXPECT_TABLE_EQ(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, UnknownOperatorThrowsException) {
  if (!IS_DEBUG) return;
  auto table_scan = std::make_shared<TableScan>(_table_wrapper, ColumnName("a"), "?!?", 1234);
  EXPECT_THROW(table_scan->execute(), std::logic_error);
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumn) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  std::map<std::string, std::set<int>> tests;
  tests["="] = {104};
  tests["!="] = {100, 102, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests["<"] = {100, 102};
  tests["<="] = {100, 102, 104};
  tests[">"] = {106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[">="] = {104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests["BETWEEN"] = {104, 106, 108};
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

  std::map<std::string, std::set<int>> tests;
  tests["="] = {104};
  tests["!="] = {100, 102, 106};
  tests["<"] = {100, 102};
  tests["<="] = {100, 102, 104};
  tests[">"] = {106};
  tests[">="] = {104, 106};
  tests["BETWEEN"] = {104, 106};
  for (const auto& test : tests) {
    auto scan1 = std::make_shared<TableScan>(_table_wrapper_even_dict, ColumnName("b"), "<", 108);
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

  auto scan_1 = std::make_shared<TableScan>(_table_wrapper_part_dict, ColumnName("a"), "<", 10);
  scan_1->execute();

  EXPECT_TABLE_EQ(scan_1->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanWeirdPosList) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_seq_filtered_onlyodd.tbl", 2);

  auto scan_1 = std::make_shared<TableScan>(_table_wrapper_filtered, ColumnName("a"), "<", 10);
  scan_1->execute();

  EXPECT_TABLE_EQ(scan_1->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumnValueGreaterMaxDictionaryValue) {
  // We compare column values with 30 which is greater than the greatest dictionary entry.
  std::map<std::string, std::set<int>> tests;
  tests["="] = {};
  tests["!="] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests["<"] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests["<="] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[">"] = {};
  tests[">="] = {};
  tests["BETWEEN"] = {};
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
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_column_parameter.tbl", 1);

  auto scan = std::make_shared<TableScan>(_table_wrapper_int3, ColumnName("b"), "=", ColumnName("a"));
  scan->execute();
  EXPECT_TABLE_EQ(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanOnDictWithColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_column_parameter.tbl", 1);

  auto scan = std::make_shared<TableScan>(_table_wrapper_int3_dict, ColumnName("b"), "=", ColumnName("a"));
  scan->execute();
  EXPECT_TABLE_EQ(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanOnDictColumnAroundBounds) {
  // scanning for a value that is around the dictionary's bounds

  std::map<std::string, std::set<int>> tests;
  tests["="] = {100};
  tests["<"] = {};
  tests["<="] = {100};
  tests[">"] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[">="] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests["!="] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests["BETWEEN"] = {100, 102, 104, 106, 108, 110};

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
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ">", 12345);
  scan_1->execute();
  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(0));

  // scan_1 produced an empty result
  auto scan_2 = std::make_shared<opossum::TableScan>(scan_1, "b", "=", 456.7);
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(0));
}

TEST_F(OperatorsTableScanTest, ScanOnWideDictionaryColumn) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper_dict_16, "a", ">", 200);
  scan_1->execute();

  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(57));

  auto scan_2 = std::make_shared<opossum::TableScan>(_table_wrapper_dict_32, "a", ">", 65500);
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(37));
}

TEST_F(OperatorsTableScanTest, NumInputTables) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ">=", 1234);
  scan_1->execute();

  EXPECT_EQ(scan_1->num_in_tables(), 1);
}

TEST_F(OperatorsTableScanTest, NumOutputTables) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ">=", 1234);

  EXPECT_EQ(scan_1->num_out_tables(), 1);
}

TEST_F(OperatorsTableScanTest, OperatorName) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ">=", 1234);

  EXPECT_EQ(scan_1->name(), "TableScan");
}

}  // namespace opossum
