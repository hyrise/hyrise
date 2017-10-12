#include "base_test.hpp"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_unique.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class UnionUniqueTest : public BaseTest {
 public:
  void SetUp() override {
    _table_10_ints = load_table("src/test/tables/10_ints.tbl", 3);
    StorageManager::get().add_table("10_ints", _table_10_ints);

    StorageManager::get().add_table("int_float4", load_table("src/test/tables/int_float4.tbl", 3));
  }

  void TearDown() override { StorageManager::get().reset(); }

  std::shared_ptr<Table> _table_10_ints;
};

TEST_F(UnionUniqueTest, SelfUnionSimple) {
  /**
   * Scan '10_ints' so that some values get excluded. UnionUnique the result with itself, and it should not change
   */

  auto get_table_a_op = std::make_shared<GetTable>("10_ints");
  auto get_table_b_op = std::make_shared<GetTable>("10_ints");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::OpGreaterThan, 24);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::OpGreaterThan, 24);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op});

  /**
   * Just an early check we're actually getting some results here
   */
  ASSERT_EQ(table_scan_a_op->get_output()->row_count(), 4);
  ASSERT_EQ(table_scan_b_op->get_output()->row_count(), 4);

  auto union_unique_op = std::make_shared<UnionUnique>(table_scan_a_op, table_scan_a_op);
  union_unique_op->execute();

  EXPECT_TABLE_EQ(table_scan_a_op->get_output(), union_unique_op->get_output());
}

TEST_F(UnionUniqueTest, SelfUnionExlusiveRanges) {
  /**
   * Scan '10_ints' once for values smaller than 10 and then for those greater than 200. Union the results. No values
   * should be discarded
   */

  auto get_table_a_op = std::make_shared<GetTable>("10_ints");
  auto get_table_b_op = std::make_shared<GetTable>("10_ints");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::OpLessThan, 10);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::OpGreaterThan, 200);
  auto union_unique_op = std::make_shared<UnionUnique>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ(union_unique_op->get_output(), load_table("src/test/tables/10_ints_exclusive_ranges.tbl", 0));
}

TEST_F(UnionUniqueTest, SelfUnionOverlappingRanges) {
  /**
   * Scan '10_ints' once for values smaller than 100 and then for those greater than 20. Union the results.
   * Result should be all values in the original table, *without introducing duplicates of rows existing in both tables*
   * This tests the actual functionality UnionUnique is intended for.
   */

  auto get_table_a_op = std::make_shared<GetTable>("10_ints");
  auto get_table_b_op = std::make_shared<GetTable>("10_ints");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::OpGreaterThan, 20);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::OpLessThan, 100);
  auto union_unique_op = std::make_shared<UnionUnique>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ(union_unique_op->get_output(), _table_10_ints);
}

TEST_F(UnionUniqueTest, SelfUnionOverlappingRangesMultipleColumns) {
  /**
   * Scan '10_ints' once for values smaller than 100 and then for those greater than 20. Union the results.
   * Result should be all values in the original table, *without introducing duplicates of rows existing in both tables*
   * This tests the actual functionality UnionUnique is intended for.
   */

  auto get_table_a_op = std::make_shared<GetTable>("int_float4");
  auto get_table_b_op = std::make_shared<GetTable>("int_float4");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::OpGreaterThan, 12345);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{1}, ScanType::OpLessThan, 400.0);
  auto union_unique_op = std::make_shared<UnionUnique>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ(union_unique_op->get_output(), load_table("src/test/tables/int_float4_overlapping_ranges.tbl", 0));
}
}