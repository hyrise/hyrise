#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsTableScanTest : public ::testing::Test {
  void SetUp() override {
    _test_table = std::make_shared<opossum::Table>(opossum::Table(2));
    _test_table->add_column("a", "int");
    _test_table->add_column("b", "float");
    _test_table->append({123, 456.7f});
    _test_table->append({1234, 457.7f});
    _test_table->append({12345, 458.7f});
    opossum::StorageManager::get().add_table("table_a", std::move(_test_table));
    _gt = std::make_shared<opossum::GetTable>("table_a");

    _test_table_dict = std::make_shared<opossum::Table>(5);
    _test_table_dict->add_column("a", "int");
    _test_table_dict->add_column("b", "int");
    for (int i = 0; i <= 24; i += 2) _test_table_dict->append({i, 100 + i});
    _test_table_dict->compress_chunk(0);
    _test_table_dict->compress_chunk(1);
    opossum::StorageManager::get().add_table("table_dict", std::move(_test_table_dict));
    _gt_dict = std::make_shared<opossum::GetTable>("table_dict");
  }

  void TearDown() override { opossum::StorageManager::get().drop_table("table_a"); }

 public:
  std::shared_ptr<opossum::Table> _test_table, _test_table_dict;
  std::shared_ptr<opossum::GetTable> _gt, _gt_dict;
};

TEST_F(OperatorsTableScanTest, DoubleScan) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_gt, "a", ">=", 1234);
  scan_1->execute();

  auto scan_2 = std::make_shared<opossum::TableScan>(scan_1, "b", "<", 457.9);
  scan_2->execute();

  EXPECT_EQ(type_cast<int>((*(scan_2->get_output()->get_chunk(0).get_column(0)))[0]), 1234);

  EXPECT_EQ(scan_2->get_output()->row_count(), (u_int)1);
}

TEST_F(OperatorsTableScanTest, SingleScanReturnsCorrectRowCount) {
  auto scan = std::make_shared<opossum::TableScan>(_gt, "a", ">=", 1234);
  scan->execute();

  EXPECT_EQ(type_cast<int>((*(scan->get_output()->get_chunk(0).get_column(0)))[0]), 1234);
  EXPECT_EQ(type_cast<int>((*(scan->get_output()->get_chunk(1).get_column(0)))[0]), 12345);
  EXPECT_NE(type_cast<int>((*(scan->get_output()->get_chunk(0).get_column(0)))[0]), 123);
  EXPECT_NE(type_cast<int>((*(scan->get_output()->get_chunk(1).get_column(0)))[0]), 123);

  EXPECT_EQ(scan->get_output()->row_count(), (size_t)2);
}

TEST_F(OperatorsTableScanTest, UnknownOperatorThrowsException) {
  EXPECT_THROW(std::make_shared<opossum::TableScan>(_gt, "a", "?!?", 1234), std::runtime_error);
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
    auto scan = std::make_shared<opossum::TableScan>(_gt_dict, "a", test.first, 4, optional<AllTypeVariant>(9));
    scan->execute();

    auto expected_copy = test.second;
    for (ChunkID chunk_id = 0; chunk_id < scan->get_output()->chunk_count(); ++chunk_id) {
      auto& chunk = scan->get_output()->get_chunk(chunk_id);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        EXPECT_EQ(expected_copy.erase(type_cast<int>((*chunk.get_column(1))[chunk_offset])), (size_t)1);
      }
    }
    EXPECT_EQ(expected_copy.size(), (size_t)0);
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
    auto scan1 = std::make_shared<opossum::TableScan>(_gt_dict, "b", "<", 108);
    scan1->execute();

    auto scan2 = std::make_shared<opossum::TableScan>(scan1, "a", test.first, 4, opossum::optional<AllTypeVariant>(9));
    scan2->execute();

    auto expected_copy = test.second;
    for (ChunkID chunk_id = 0; chunk_id < scan2->get_output()->chunk_count(); ++chunk_id) {
      auto& chunk = scan2->get_output()->get_chunk(chunk_id);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        EXPECT_EQ(expected_copy.erase(type_cast<int>((*chunk.get_column(1))[chunk_offset])), (size_t)1);
      }
    }
    EXPECT_EQ(expected_copy.size(), (size_t)0);
  }
}

}  // namespace opossum
