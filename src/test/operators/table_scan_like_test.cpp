#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "expression/evaluation/like_matcher.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_scan/like_table_scan_impl.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

class OperatorsTableScanLikeTest : public BaseTest, public ::testing::WithParamInterface<EncodingType> {
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

    // load special chars table
    std::shared_ptr<Table> test_table_special_chars =
        load_table("src/test/tables/int_string_like_special_chars.tbl", 2);
    StorageManager::get().add_table("table_special_chars", std::move(test_table_special_chars));
    _gt_special_chars = std::make_shared<GetTable>("table_special_chars");
    _gt_special_chars->execute();

    // load and compress string table
    if (::testing::UnitTest::GetInstance()->current_test_info()->value_param()) {
      // Not all tests are parameterized - only those using compressed columns are. We have to ask the testing
      // framework if a parameter is set. Otherwise, GetParam would fail.
      auto test_table_string_compressed = load_table("src/test/tables/int_string_like.tbl", 5);
      std::vector<ChunkEncodingSpec> spec = {{EncodingType::Unencoded, GetParam()},
                                             {EncodingType::Unencoded, GetParam()}};
      ChunkEncoder::encode_all_chunks(test_table_string_compressed, spec);

      StorageManager::get().add_table("table_string_compressed", test_table_string_compressed);

      _gt_string_compressed = std::make_shared<GetTable>("table_string_compressed");
      _gt_string_compressed->execute();
    }
  }

  std::shared_ptr<GetTable> _gt, _gt_special_chars, _gt_string, _gt_string_compressed;
};

auto formatter = [](const ::testing::TestParamInfo<EncodingType> info) {
  return std::to_string(static_cast<uint32_t>(info.param));
};

INSTANTIATE_TEST_CASE_P(EncodingTypes, OperatorsTableScanLikeTest,
                        ::testing::Values(EncodingType::Unencoded, EncodingType::Dictionary,
                                          EncodingType::FixedStringDictionary, EncodingType::RunLength),
                        formatter);

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

TEST_P(OperatorsTableScanLikeTest, ScanLikeEmptyStringOnDict) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{1}, PredicateCondition::Like, "%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanLikeTest, ScanLikeUnderscoreWildcard) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_starting.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::Like, "%D%_m_f%");
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

TEST_P(OperatorsTableScanLikeTest, ScanLikeEmptyStringDict) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{1}, PredicateCondition::Like, "%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanLikeTest, ScanLikeStartingOnDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_starting.tbl", 1);
  auto scan = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{1}, PredicateCondition::Like, "Dampf%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanLikeTest, ScanLikeStartingOnReferencedDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_starting.tbl", 1);
  auto scan1 = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{0}, PredicateCondition::GreaterThan, 0);
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

TEST_P(OperatorsTableScanLikeTest, ScanLikeEndingOnDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_ending.tbl", 1);
  auto scan =
      std::make_shared<TableScan>(_gt_string_compressed, ColumnID{1}, PredicateCondition::Like, "%gesellschaft");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanLikeTest, ScanLikeEndingOnReferencedDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_ending.tbl", 1);
  auto scan1 = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{0}, PredicateCondition::GreaterThan, 0);
  scan1->execute();
  auto scan2 = std::make_shared<TableScan>(scan1, ColumnID{1}, PredicateCondition::Like, "%gesellschaft");
  scan2->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan2->get_output(), expected_result);
}

TEST_F(OperatorsTableScanLikeTest, ScanLikeOnSpecialChars) {
  std::shared_ptr<Table> expected_result_1 = load_table("src/test/tables/int_string_like_special_chars_1.tbl", 1);
  std::shared_ptr<Table> expected_result_2 = load_table("src/test/tables/int_string_like_special_chars_2.tbl", 1);
  std::shared_ptr<Table> expected_result_4 = load_table("src/test/tables/int_string_like_special_chars_3.tbl", 1);

  auto scan1 = std::make_shared<TableScan>(_gt_special_chars, ColumnID{1}, PredicateCondition::Like, "%2^2%");
  scan1->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan1->get_output(), expected_result_1);

  auto scan2 = std::make_shared<TableScan>(_gt_special_chars, ColumnID{1}, PredicateCondition::Like, "%$%$%");
  scan2->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan2->get_output(), expected_result_1);

  std::shared_ptr<Table> expected_result2 = load_table("src/test/tables/int_string_like_special_chars_2.tbl", 1);
  auto scan3 = std::make_shared<TableScan>(_gt_special_chars, ColumnID{1}, PredicateCondition::Like, "%(%)%");
  scan3->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan3->get_output(), expected_result_2);

  auto scan4 =
      std::make_shared<TableScan>(_gt_special_chars, ColumnID{1}, PredicateCondition::Like, "%la\\.^$+?)({}.*__bl%");
  scan4->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan4->get_output(), expected_result_4);
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

TEST_P(OperatorsTableScanLikeTest, ScanLikeContainingOnDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_containing.tbl", 1);
  auto scan = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{1}, PredicateCondition::Like,
                                          "%schifffahrtsgesellschaft%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanLikeTest, ScanLikeContainingOnReferencedDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_containing.tbl", 1);
  auto scan1 = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{0}, PredicateCondition::GreaterThan, 0);
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

TEST_P(OperatorsTableScanLikeTest, ScanLikeNotFoundOnDictColumn) {
  auto scan = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{1}, PredicateCondition::Like, "%not_there%");
  scan->execute();
  EXPECT_EQ(scan->get_output()->row_count(), 0u);
}

TEST_P(OperatorsTableScanLikeTest, ScanLikeNotFoundOnReferencedDictColumn) {
  auto scan1 = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{0}, PredicateCondition::GreaterThan, 0);
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

TEST_P(OperatorsTableScanLikeTest, ScanNotLikeEmptyStringOnDict) {
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{1}, PredicateCondition::NotLike, "%");
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

TEST_P(OperatorsTableScanLikeTest, ScanNotLikeAllRowsOnDict) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{1}, PredicateCondition::NotLike, "%foo%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_F(OperatorsTableScanLikeTest, ScanNotLikeUnderscoreWildcard) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_not_starting.tbl", 1);
  // wildcard has to be placed at front and/or back of search string
  auto scan = std::make_shared<TableScan>(_gt_string, ColumnID{1}, PredicateCondition::NotLike, "D_m_f%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanLikeTest, ScanNotLikeUnderscoreWildcardOnDict) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_like_not_starting.tbl", 1);
  // wildcard has to be placed at front and/or back of search
  auto scan = std::make_shared<TableScan>(_gt_string_compressed, ColumnID{1}, PredicateCondition::NotLike, "D_m_f%");
  scan->execute();
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

}  // namespace opossum
