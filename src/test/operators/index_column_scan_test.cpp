#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/index_column_scan.hpp"
#include "../../lib/operators/table_wrapper.hpp"
#include "../../lib/storage/dictionary_compression.hpp"
#include "../../lib/storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "../../lib/storage/index/group_key/composite_group_key_index.hpp"
#include "../../lib/storage/index/group_key/group_key_index.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

template <typename DerivedIndex>
class OperatorsIndexColumnScanTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));

    std::shared_ptr<Table> test_table_dict = std::make_shared<Table>(5);
    test_table_dict->add_column("a", "int");
    test_table_dict->add_column("b", "int");

    for (int i = 0; i <= 24; i += 2) test_table_dict->append({i, 100 + i});

    DictionaryCompression::compress_chunks(*test_table_dict, {ChunkID{0}, ChunkID{1}});

    test_table_dict->get_chunk(ChunkID{0})
        .create_index<DerivedIndex>({test_table_dict->get_chunk(ChunkID{0}).get_column(ColumnID{0})});
    test_table_dict->get_chunk(ChunkID{0})
        .create_index<DerivedIndex>({test_table_dict->get_chunk(ChunkID{0}).get_column(ColumnID{1})});

    _table_wrapper_dict = std::make_shared<TableWrapper>(std::move(test_table_dict));

    _table_wrapper->execute();
    _table_wrapper_dict->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_dict;
};

// List of indices to test
typedef ::testing::Types<GroupKeyIndex, AdaptiveRadixTreeIndex, CompositeGroupKeyIndex /* add further indices */>
    DerivedIndices;
TYPED_TEST_CASE(OperatorsIndexColumnScanTest, DerivedIndices);

TYPED_TEST(OperatorsIndexColumnScanTest, DoubleScan) {
  std::shared_ptr<Table> expected_result = this->load_table("src/test/tables/int_float_filtered.tbl", 2);

  auto scan_1 =
      std::make_shared<IndexColumnScan>(this->_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);
  scan_1->execute();

  auto scan_2 = std::make_shared<IndexColumnScan>(scan_1, ColumnID{1}, ScanType::OpLessThan, 457.9);
  scan_2->execute();

  this->EXPECT_TABLE_EQ(scan_2->get_output(), expected_result);
}

TYPED_TEST(OperatorsIndexColumnScanTest, DoubleScanOffsetPosition) {
  auto scan1 = std::make_shared<IndexColumnScan>(this->_table_wrapper_dict, ColumnID{0}, ScanType::OpGreaterThan, 10);
  scan1->execute();
  auto scan2 = std::make_shared<IndexColumnScan>(scan1, ColumnID{1}, ScanType::OpEquals, 118);
  scan2->execute();

  auto& chunk = scan2->get_output()->get_chunk(ChunkID{1});
  EXPECT_EQ(type_cast<int>((*chunk.get_column(ColumnID{0}))[0]), 18);
  EXPECT_EQ(type_cast<int>((*chunk.get_column(ColumnID{1}))[0]), 118);
}

TYPED_TEST(OperatorsIndexColumnScanTest, SingleScan) {
  std::shared_ptr<Table> expected_result = this->load_table("src/test/tables/int_float_filtered2.tbl", 1);

  auto scan = std::make_shared<IndexColumnScan>(this->_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);
  scan->execute();

  this->EXPECT_TABLE_EQ(scan->get_output(), expected_result);
}

TYPED_TEST(OperatorsIndexColumnScanTest, LikeOperatorThrowsException) {
  if (!IS_DEBUG) return;
  auto table_scan = std::make_shared<IndexColumnScan>(this->_table_wrapper, ColumnID{0}, ScanType::OpLike, 1234);
  EXPECT_THROW(table_scan->execute(), std::logic_error);
}

TYPED_TEST(OperatorsIndexColumnScanTest, ScanOnDictColumn) {
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
    auto scan = std::make_shared<IndexColumnScan>(this->_table_wrapper_dict, ColumnID{0}, test.first, 4,
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

TYPED_TEST(OperatorsIndexColumnScanTest, ScanOnReferencedDictColumn) {
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
    auto scan1 = std::make_shared<IndexColumnScan>(this->_table_wrapper_dict, ColumnID{1}, ScanType::OpLessThan, 108);
    scan1->execute();

    auto scan2 = std::make_shared<IndexColumnScan>(scan1, ColumnID{0}, test.first, 4, optional<AllTypeVariant>(9));
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

}  // namespace opossum
