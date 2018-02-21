#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsTableScanLikeTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> test_table = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(test_table));
    _gt = std::make_shared<GetTable>("table_a");
    _gt->execute();

    // load string table
    std::shared_ptr<Table> test_table_string = load_table("src/test/tables/int_string_like.tbl", 2);
    StorageManager::get().add_table("table_string", std::move(test_table_string));
    _gt_string = std::make_shared<GetTable>("table_string");
    _gt_string->execute();

    // load and compress string table
    auto test_table_string_dict = load_table("src/test/tables/int_string_like.tbl", 5);
    ChunkEncoder::encode_chunks(test_table_string_dict, {ChunkID{0}});

    StorageManager::get().add_table("table_string_dict", test_table_string_dict);

    _gt_string_dict = std::make_shared<GetTable>("table_string_dict");
    _gt_string_dict->execute();
  }

  std::shared_ptr<GetTable> _gt, _gt_string, _gt_string_dict;
};

/*
    Tests for operator PredicateCondition::Like
    The **%** sign is used to define wildcards (missing letters) both before and after the search pattern.
    We expect the operator to be run on a string column using a string value with wildcard.
*/
TEST_F(OperatorsTableScanLikeTest, ScanLikeNonStringColumn) {
  auto scan = std::make_shared<TableScan>(_gt, ColumnID{0}, PredicateCondition::Like, "%test");
  EXPECT_THROW(scan->execute(), std::exception);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeNonStringValue) {
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, 1234);
  scan->execute();
  EXPECT_EQ(scan->get_output()->row_count(), 1u);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeEmptyString) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, "%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeEmptyStringOnDict) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string_dict, ColumnID{1}, PredicateCondition::Like, "%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeCaseInsensitivity) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_starting.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, "dAmpF%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanLikeTest, ScanLikeUnderscoreWildcard) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_starting.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, "d_m_f%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

// PredicateCondition::Like - Starting
TEST_F(OperatorsTableScanLikeTest, ScanLike_Starting) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_starting.tbl", 1);
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, "Dampf%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeEmptyStringDict) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string_dict, ColumnID{1}, PredicateCondition::Like, "%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeStartingOnDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_starting.tbl", 1);
  auto scan = std::make_shared<TableScan>(_gt_string_dict, ColumnID{1}, PredicateCondition::Like, "Dampf%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeStartingOnReferencedDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_starting.tbl", 1);
  auto scan1 = std::make_shared<TableScan>(_gt_string_dict, ColumnID{0}, PredicateCondition::GreaterThan, 0);
  scan1->execute();
  auto scan2 = std::make_shared<TableScan>(scan1, ColumnID{1}, PredicateCondition::Like, "Dampf%");
  scan2->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan2->get_output(), expected_result);
}
// PredicateCondition::Like - Ending
TEST_F(OperatorsTableScanLikeTest, ScanLikeEnding) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_ending.tbl", 1);
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, "%gesellschaft");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeEndingOnDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_ending.tbl", 1);
  auto scan = std::make_shared<TableScan>(_gt_string_dict, ColumnID{1}, PredicateCondition::Like, "%gesellschaft");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeEndingOnReferencedDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_ending.tbl", 1);
  auto scan1 = std::make_shared<TableScan>(_gt_string_dict, ColumnID{0}, PredicateCondition::GreaterThan, 0);
  scan1->execute();
  auto scan2 = std::make_shared<TableScan>(scan1, ColumnID{1}, PredicateCondition::Like, "%gesellschaft");
  scan2->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan2->get_output(), expected_result);
}

// PredicateCondition::Like - Containing Wildcard
TEST_F(OperatorsTableScanLikeTest, ScanLikeContainingWildcard) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_containing_wildcard.tbl", 1);
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, "Schiff%schaft");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

// PredicateCondition::Like - Containing
TEST_F(OperatorsTableScanLikeTest, ScanLikeContaining) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_containing.tbl", 1);
  auto scan =
      std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, "%schifffahrtsgesellschaft%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeContainingOnDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_containing.tbl", 1);
  auto scan =
      std::make_shared<TableScan>(_gt_string_dict, ColumnID{1}, PredicateCondition::Like, "%schifffahrtsgesellschaft%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeContainingOnReferencedDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_containing.tbl", 1);
  auto scan1 = std::make_shared<TableScan>(_gt_string_dict, ColumnID{0}, PredicateCondition::GreaterThan, 0);
  scan1->execute();
  auto scan2 = std::make_shared<TableScan>(scan1, ColumnID{1}, PredicateCondition::Like, "%schifffahrtsgesellschaft%");
  scan2->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan2->get_output(), expected_result);
}
// PredicateCondition::Like - Not Found
TEST_F(OperatorsTableScanLikeTest, ScanLikeNotFound) {
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, "%not_there%");
  scan->execute();
  EXPECT_EQ(scan->get_output()->row_count(), 0u);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeNotFoundOnDictColumn) {
  auto scan = std::make_shared<TableScan>(_gt_string_dict, ColumnID{1}, PredicateCondition::Like, "%not_there%");
  scan->execute();
  EXPECT_EQ(scan->get_output()->row_count(), 0u);
}
TEST_F(OperatorsTableScanLikeTest, ScanLikeNotFoundOnReferencedDictColumn) {
  auto scan1 = std::make_shared<TableScan>(_gt_string_dict, ColumnID{0}, PredicateCondition::GreaterThan, 0);
  scan1->execute();
  auto scan2 = std::make_shared<TableScan>(scan1, ColumnID{1}, PredicateCondition::Like, "%not_there%");
  scan2->execute();
  EXPECT_EQ(scan2->get_output()->row_count(), 0u);
}
// PredicateCondition::NotLike
TEST_F(OperatorsTableScanLikeTest, ScanNotLikeEmptyString) {
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::NotLike, "%");
  scan->execute();
  EXPECT_EQ(scan->get_output()->row_count(), 0u);
}
TEST_F(OperatorsTableScanLikeTest, ScanNotLikeEmptyStringOnDict) {
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string_dict, ColumnID{1}, PredicateCondition::NotLike, "%");
  scan->execute();
  EXPECT_EQ(scan->get_output()->row_count(), 0u);
}
TEST_F(OperatorsTableScanLikeTest, ScanNotLikeAllRows) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::NotLike, "%foo%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanNotLikeAllRowsOnDict) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string_dict, ColumnID{1}, PredicateCondition::NotLike, "%foo%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanLikeTest, ScanNotLikeUnderscoreWildcard) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_not_starting.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::NotLike, "d_m_f%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}
TEST_F(OperatorsTableScanLikeTest, ScanNotLikeUnderscoreWildcardOnDict) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_not_starting.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string_dict, ColumnID{1}, PredicateCondition::NotLike, "d_m_f%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

}  // namespace opossum
