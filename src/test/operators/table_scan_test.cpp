#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsTableScanTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> test_table = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(test_table));
    _gt = std::make_shared<GetTable>("table_a");

    std::shared_ptr<Table> test_even_dict = std::make_shared<Table>(5);
    test_even_dict->add_column("a", "int");
    test_even_dict->add_column("b", "int");
    for (int i = 0; i <= 24; i += 2) test_even_dict->append({i, 100 + i});
    test_even_dict->compress_chunk(0);
    test_even_dict->compress_chunk(1);
    StorageManager::get().add_table("table_even_dict", std::move(test_even_dict));

    _gt_even_dict = std::make_shared<GetTable>("table_even_dict");

    _gt->execute();
    _gt_even_dict->execute();

    std::shared_ptr<Table> test_table_dict = std::make_shared<Table>(5);
    test_table_dict->add_column("a", "int");
    test_table_dict->add_column("b", "float");
    for (int i = 1; i < 20; ++i) test_table_dict->append({i, 100.1 + i});
    test_table_dict->compress_chunk(0);
    test_table_dict->compress_chunk(2);
    StorageManager::get().add_table("table_part_dict", test_table_dict);
    _gt_part_dict = std::make_shared<GetTable>("table_part_dict");
    _gt_part_dict->execute();

    std::shared_ptr<Table> test_table_filtered = std::make_shared<Table>(5);
    test_table_filtered->add_column("a", "int", false);
    test_table_filtered->add_column("b", "float", false);
    auto pos_list = std::make_shared<PosList>();
    pos_list->emplace_back(test_table_dict->calculate_row_id(3, 1));
    pos_list->emplace_back(test_table_dict->calculate_row_id(2, 0));
    pos_list->emplace_back(test_table_dict->calculate_row_id(1, 1));
    pos_list->emplace_back(test_table_dict->calculate_row_id(3, 3));
    pos_list->emplace_back(test_table_dict->calculate_row_id(1, 3));
    pos_list->emplace_back(test_table_dict->calculate_row_id(0, 2));
    pos_list->emplace_back(test_table_dict->calculate_row_id(2, 2));
    pos_list->emplace_back(test_table_dict->calculate_row_id(2, 4));
    pos_list->emplace_back(test_table_dict->calculate_row_id(0, 0));
    pos_list->emplace_back(test_table_dict->calculate_row_id(0, 4));
    auto col_a = std::make_shared<ReferenceColumn>(test_table_dict, 0, pos_list);
    auto col_b = std::make_shared<ReferenceColumn>(test_table_dict, 1, pos_list);
    Chunk chunk;
    chunk.add_column(col_a);
    chunk.add_column(col_b);
    test_table_filtered->add_chunk(std::move(chunk));
    StorageManager::get().add_table("table_filtered", test_table_filtered);
    _gt_filtered = std::make_shared<GetTable>("table_filtered");
    _gt_filtered->execute();
  }

  std::shared_ptr<GetTable> _gt, _gt_even_dict, _gt_part_dict, _gt_filtered;
};

TEST_F(OperatorsTableScanTest, DoubleScan) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);

  auto scan_1 = std::make_shared<TableScan>(_gt, "a", ">=", 1234);
  scan_1->execute();

  auto scan_2 = std::make_shared<TableScan>(scan_1, "b", "<", 457.9);
  scan_2->execute();

  EXPECT_TABLE_EQ(scan_2->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, SingleScanReturnsCorrectRowCount) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);

  auto scan = std::make_shared<TableScan>(_gt, "a", ">=", 1234);
  scan->execute();

  EXPECT_TABLE_EQ(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, UnknownOperatorThrowsException) {
  auto table_scan = std::make_shared<TableScan>(_gt, "a", "?!?", 1234);
  EXPECT_THROW(table_scan->execute(), std::runtime_error);
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
    auto scan = std::make_shared<TableScan>(_gt_even_dict, "a", test.first, 4, optional<AllTypeVariant>(9));
    scan->execute();

    auto expected_copy = test.second;
    for (ChunkID chunk_id = 0; chunk_id < scan->get_output()->chunk_count(); ++chunk_id) {
      auto& chunk = scan->get_output()->get_chunk(chunk_id);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        EXPECT_EQ(expected_copy.erase(type_cast<int>((*chunk.get_column(1))[chunk_offset])), 1ull);
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
    auto scan1 = std::make_shared<TableScan>(_gt_even_dict, "b", "<", 108);
    scan1->execute();

    auto scan2 = std::make_shared<TableScan>(scan1, "a", test.first, 4, optional<AllTypeVariant>(9));
    scan2->execute();

    auto expected_copy = test.second;
    for (ChunkID chunk_id = 0; chunk_id < scan2->get_output()->chunk_count(); ++chunk_id) {
      auto& chunk = scan2->get_output()->get_chunk(chunk_id);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        EXPECT_EQ(expected_copy.erase(type_cast<int>((*chunk.get_column(1))[chunk_offset])), 1ull);
      }
    }
    EXPECT_EQ(expected_copy.size(), (size_t)0);
  }
}

TEST_F(OperatorsTableScanTest, ScanPartiallyCompressed) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_seq_filtered.tbl", 2);

  auto scan_1 = std::make_shared<TableScan>(_gt_part_dict, "a", "<", 10);
  scan_1->execute();

  EXPECT_TABLE_EQ(scan_1->get_output(), expected_result);
}

TEST_F(OperatorsTableScanTest, ScanWeirdPosList) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_seq_filtered_onlyodd.tbl", 2);

  auto scan_1 = std::make_shared<TableScan>(_gt_filtered, "a", "<", 10);
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
    auto scan = std::make_shared<TableScan>(_gt_even_dict, "a", test.first, 30, optional<AllTypeVariant>(34));
    scan->execute();

    auto expected_copy = test.second;
    for (ChunkID chunk_id = 0; chunk_id < scan->get_output()->chunk_count(); ++chunk_id) {
      auto& chunk = scan->get_output()->get_chunk(chunk_id);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        EXPECT_EQ(expected_copy.erase(type_cast<int>((*chunk.get_column(1))[chunk_offset])), 1ull);
      }
    }
    EXPECT_EQ(expected_copy.size(), 0ull);
  }
}

}  // namespace opossum
