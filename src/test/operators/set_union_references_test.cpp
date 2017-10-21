#include <memory>

#include "base_test.hpp"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/set_union_references.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/print.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SetUnionTest : public BaseTest {
 public:
  void SetUp() override {
    _table_10_ints = load_table("src/test/tables/10_ints.tbl", 3);
    StorageManager::get().add_table("10_ints", _table_10_ints);

    StorageManager::get().add_table("int_float4", load_table("src/test/tables/int_float4.tbl", 3));
    StorageManager::get().add_table("int_int", load_table("src/test/tables/int_int.tbl", 2));
  }

  void TearDown() override { StorageManager::get().reset(); }

  std::shared_ptr<Table> _table_10_ints;
};

TEST_F(SetUnionTest, SelfUnionSimple) {
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
  ASSERT_EQ(table_scan_a_op->get_output()->row_count(), 4u);
  ASSERT_EQ(table_scan_b_op->get_output()->row_count(), 4u);

  auto union_unique_op = std::make_shared<SetUnionReferences>(table_scan_a_op, table_scan_a_op);
  union_unique_op->execute();

  EXPECT_TABLE_EQ(table_scan_a_op->get_output(), union_unique_op->get_output());
}

TEST_F(SetUnionTest, SelfUnionExlusiveRanges) {
  /**
   * Scan '10_ints' once for values smaller than 10 and then for those greater than 200. Union the results. No values
   * should be discarded
   */

  auto get_table_a_op = std::make_shared<GetTable>("10_ints");
  auto get_table_b_op = std::make_shared<GetTable>("10_ints");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::OpLessThan, 10);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::OpGreaterThan, 200);
  auto union_unique_op = std::make_shared<SetUnionReferences>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ(union_unique_op->get_output(), load_table("src/test/tables/10_ints_exclusive_ranges.tbl", 0));
}

TEST_F(SetUnionTest, SelfUnionOverlappingRanges) {
  /**
   * Scan '10_ints' once for values smaller than 100 and then for those greater than 20. Union the results.
   * Result should be all values in the original table, *without introducing duplicates of rows existing in both tables*
   * This tests the actual functionality UnionUnique is intended for.
   */

  auto get_table_a_op = std::make_shared<GetTable>("10_ints");
  auto get_table_b_op = std::make_shared<GetTable>("10_ints");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::OpGreaterThan, 20);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::OpLessThan, 100);
  auto union_unique_op = std::make_shared<SetUnionReferences>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ(union_unique_op->get_output(), _table_10_ints);
}

TEST_F(SetUnionTest, SelfUnionOverlappingRangesMultipleColumns) {
  /**
   * Scan '10_ints' once for values smaller than 100 and then for those greater than 20. Union the results.
   * Result should be all values in the original table, *without introducing duplicates of rows existing in both tables*
   * This tests the actual functionality SetUnion is intended for.
   */

  auto get_table_a_op = std::make_shared<GetTable>("int_float4");
  auto get_table_b_op = std::make_shared<GetTable>("int_float4");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::OpGreaterThan, 12345);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{1}, ScanType::OpLessThan, 400.0);
  auto union_unique_op = std::make_shared<SetUnionReferences>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ(union_unique_op->get_output(), load_table("src/test/tables/int_float4_overlapping_ranges.tbl", 0));
}

TEST_F(SetUnionTest, MultipleReferencedTables) {
  /**
   * Join int_float4 and int_int on their respective "a" column. Scan the result once for int_int.b >= 2 and for
   * int_float.a < 457. SetUnion the results.
   * The joins will create input tables with multiple referenced tables which tests the column segmenting aspect of
   * the SetUnion algorithm.
   *
   * Result of the JOIN:
   *   |       a|       b|       a|       b|
   *   |     int|   float|     int|     int|
   *   === Chunk 0 ===
   *   |   12345|   457.7|   12345|       1|
   *   |   12345|   456.7|   12345|       1|
   *   === Chunk 1 ===
   *   |     123|   458.7|     123|       2|
   *
   * Result of the Scan int_int.b >= 2
   *   === Columns
   *   |       a|       b|       a|       b|
   *   |     int|   float|     int|     int|
   *   === Chunk 0 ===
   *   |     123|   458.7|     123|       2|
   *
   * Result of the Scan int_float.a < 457
   *   === Columns
   *   |       a|       b|       a|       b|
   *   |     int|   float|     int|     int|
   *   === Chunk 0 ===
   *   |   12345|   456.7|   12345|       1|
   *
   */

  auto get_table_a_op = std::make_shared<GetTable>("int_float4");
  auto get_table_b_op = std::make_shared<GetTable>("int_int");
  auto get_table_c_op = std::make_shared<GetTable>("int_float4");
  auto get_table_d_op = std::make_shared<GetTable>("int_int");
  auto join_a = std::make_shared<JoinNestedLoopA>(get_table_a_op, get_table_b_op, JoinMode::Inner,
                                                  std::make_pair(ColumnID{0}, ColumnID{0}), ScanType::OpEquals);
  auto join_b = std::make_shared<JoinNestedLoopA>(get_table_c_op, get_table_d_op, JoinMode::Inner,
                                                  std::make_pair(ColumnID{0}, ColumnID{0}), ScanType::OpEquals);

  auto table_scan_a_op = std::make_shared<TableScan>(join_a, ColumnID{3}, ScanType::OpGreaterThanEquals, 2);
  auto table_scan_b_op = std::make_shared<TableScan>(join_b, ColumnID{1}, ScanType::OpLessThan, 457.0);
  auto union_unique_op = std::make_shared<SetUnionReferences>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, get_table_c_op, get_table_d_op, join_a, join_b, table_scan_a_op,
                table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ(union_unique_op->get_output(), load_table("src/test/tables/int_float4_int_int_set_union.tbl", 0));

  /**
   * Additionally check that Column 0 and 1 have the same pos list and that Column 2 and 3 have the same pos list to
   * make sure we're not creating redundant data.
   */
  const auto get_pos_list = [](const auto & table, ColumnID column_id) {
    const auto column = table->get_chunk(ChunkID{0}).get_column(column_id);
    const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);
    return ref_column->pos_list();
  };

  const auto &output = union_unique_op->get_output();

  EXPECT_EQ(get_pos_list(output, ColumnID{0}), get_pos_list(output, ColumnID{1}));
  EXPECT_EQ(get_pos_list(output, ColumnID{2}), get_pos_list(output, ColumnID{3}));
}


}  // namespace opossum
