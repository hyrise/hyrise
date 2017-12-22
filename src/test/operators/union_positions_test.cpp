#include <memory>
#include <utility>

#include "base_test.hpp"

#include "operators/get_table.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "storage/reference_column.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class UnionPositionsTest : public BaseTest {
 public:
  void SetUp() override {
    _table_10_ints = load_table("src/test/tables/10_ints.tbl", 3);
    StorageManager::get().add_table("10_ints", _table_10_ints);

    _table_int_float4 = load_table("src/test/tables/int_float4.tbl", 3);
    StorageManager::get().add_table("int_float4", _table_int_float4);
    StorageManager::get().add_table("int_int", load_table("src/test/tables/int_int.tbl", 2));
  }

  std::shared_ptr<Table> _table_10_ints;
  std::shared_ptr<Table> _table_int_float4;
};

TEST_F(UnionPositionsTest, SelfUnionSimple) {
  /**
   * Scan '10_ints' so that some values get excluded. UnionUnique the result with itself, and it should not change
   */

  auto get_table_a_op = std::make_shared<GetTable>("10_ints");
  auto get_table_b_op = std::make_shared<GetTable>("10_ints");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::GreaterThan, 24);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::GreaterThan, 24);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op});

  /**
   * Just an early check we're actually getting some results here
   */
  ASSERT_EQ(table_scan_a_op->get_output()->row_count(), 4u);
  ASSERT_EQ(table_scan_b_op->get_output()->row_count(), 4u);

  auto union_unique_op = std::make_shared<UnionPositions>(table_scan_a_op, table_scan_a_op);
  union_unique_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(table_scan_a_op->get_output(), union_unique_op->get_output());
}

TEST_F(UnionPositionsTest, SelfUnionExlusiveRanges) {
  /**
   * Scan '10_ints' once for values smaller than 10 and then for those greater than 200. Union the results. No values
   * should be discarded
   */

  auto get_table_a_op = std::make_shared<GetTable>("10_ints");
  auto get_table_b_op = std::make_shared<GetTable>("10_ints");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::LessThan, 10);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::GreaterThan, 200);
  auto union_unique_op = std::make_shared<UnionPositions>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ_UNORDERED(union_unique_op->get_output(),
                            load_table("src/test/tables/10_ints_exclusive_ranges.tbl", Chunk::MAX_SIZE));
}

TEST_F(UnionPositionsTest, SelfUnionOverlappingRanges) {
  /**
   * Scan '10_ints' once for values smaller than 100 and then for those greater than 20. Union the results.
   * Result should be all values in the original table, *without introducing duplicates of rows existing in both tables*
   * This tests the actual functionality UnionUnique is intended for.
   */

  auto get_table_a_op = std::make_shared<GetTable>("10_ints");
  auto get_table_b_op = std::make_shared<GetTable>("10_ints");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::GreaterThan, 20);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::LessThan, 100);
  auto union_unique_op = std::make_shared<UnionPositions>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ_UNORDERED(union_unique_op->get_output(), _table_10_ints);
}

TEST_F(UnionPositionsTest, EarlyResultLeft) {
  /**
   * If one of the input tables is empty, an early result should be produced
   */

  auto get_table_a_op = std::make_shared<GetTable>("int_float4");
  auto get_table_b_op = std::make_shared<GetTable>("int_float4");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::LessThan, 12346);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::LessThan, 0);
  auto union_unique_op = std::make_shared<UnionPositions>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ_UNORDERED(union_unique_op->get_output(),
                            load_table("src/test/tables/int_float2.tbl", Chunk::MAX_SIZE));
  EXPECT_EQ(table_scan_a_op->get_output(), union_unique_op->get_output());
}

TEST_F(UnionPositionsTest, EarlyResultRight) {
  /**
   * If one of the input tables is empty, an early result should be produced
   */

  auto get_table_a_op = std::make_shared<GetTable>("int_float4");
  auto get_table_b_op = std::make_shared<GetTable>("int_float4");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::LessThan, 0);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{0}, ScanType::LessThan, 12346);
  auto union_unique_op = std::make_shared<UnionPositions>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ_UNORDERED(union_unique_op->get_output(),
                            load_table("src/test/tables/int_float2.tbl", Chunk::MAX_SIZE));
  EXPECT_EQ(table_scan_b_op->get_output(), union_unique_op->get_output());
}

TEST_F(UnionPositionsTest, SelfUnionOverlappingRangesMultipleColumns) {
  /**
   * Scan '10_ints' once for values smaller than 100 and then for those greater than 20. Union the results.
   * Result should be all values in the original table, *without introducing duplicates of rows existing in both tables*
   * This tests the actual functionality UnionPositions is intended for.
   */

  auto get_table_a_op = std::make_shared<GetTable>("int_float4");
  auto get_table_b_op = std::make_shared<GetTable>("int_float4");
  auto table_scan_a_op = std::make_shared<TableScan>(get_table_a_op, ColumnID{0}, ScanType::GreaterThan, 12345);
  auto table_scan_b_op = std::make_shared<TableScan>(get_table_b_op, ColumnID{1}, ScanType::LessThan, 400.0);
  auto union_unique_op = std::make_shared<UnionPositions>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, table_scan_a_op, table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ_UNORDERED(union_unique_op->get_output(),
                            load_table("src/test/tables/int_float4_overlapping_ranges.tbl", Chunk::MAX_SIZE));
}

TEST_F(UnionPositionsTest, MultipleReferencedTables) {
  /**
   * Join int_float4 and int_int on their respective "a" column. Scan the result once for int_int.b >= 2 and for
   * int_float.a < 457. UnionPositions the results.
   * The joins will create input tables with multiple referenced tables which tests the column segmenting aspect of
   * the UnionPositions algorithm.
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
  auto join_a = std::make_shared<JoinNestedLoop>(get_table_a_op, get_table_b_op, JoinMode::Inner,
                                                 std::make_pair(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  auto join_b = std::make_shared<JoinNestedLoop>(get_table_c_op, get_table_d_op, JoinMode::Inner,
                                                 std::make_pair(ColumnID{0}, ColumnID{0}), ScanType::Equals);

  auto table_scan_a_op = std::make_shared<TableScan>(join_a, ColumnID{3}, ScanType::GreaterThanEquals, 2);
  auto table_scan_b_op = std::make_shared<TableScan>(join_b, ColumnID{1}, ScanType::LessThan, 457.0);
  auto union_unique_op = std::make_shared<UnionPositions>(table_scan_a_op, table_scan_b_op);

  _execute_all({get_table_a_op, get_table_b_op, get_table_c_op, get_table_d_op, join_a, join_b, table_scan_a_op,
                table_scan_b_op, union_unique_op});

  EXPECT_TABLE_EQ_UNORDERED(union_unique_op->get_output(),
                            load_table("src/test/tables/int_float4_int_int_union_positions.tbl", Chunk::MAX_SIZE));

  /**
   * Additionally check that Column 0 and 1 have the same pos list and that Column 2 and 3 have the same pos list to
   * make sure we're not creating redundant data.
   */
  const auto get_pos_list = [](const auto& table, ColumnID column_id) {
    const auto column = table->get_chunk(ChunkID{0}).get_column(column_id);
    const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);
    return *ref_column->pos_list();
  };

  const auto& output = union_unique_op->get_output();

  EXPECT_EQ(get_pos_list(output, ColumnID{0}), get_pos_list(output, ColumnID{1}));
  EXPECT_EQ(get_pos_list(output, ColumnID{2}), get_pos_list(output, ColumnID{3}));
}

TEST_F(UnionPositionsTest, MultipleShuffledPosList) {
  /**
   * Test UnionPositions on Tables with multiple shuffled poslists and columns sharing poslists
   *
   * TODO(anybody) this test is an atrocity, look how complicated it is to build Reference Tables!
   */
  // Left input table, chunk 0, pos_list 0
  auto pos_list_left_0_0 = std::make_shared<PosList>();
  pos_list_left_0_0->emplace_back(RowID{ChunkID{1}, 2});
  pos_list_left_0_0->emplace_back(RowID{ChunkID{0}, 1});
  pos_list_left_0_0->emplace_back(RowID{ChunkID{1}, 2});

  // Left input table, chunk 1, pos_list 0
  auto pos_list_left_1_0 = std::make_shared<PosList>();
  pos_list_left_1_0->emplace_back(RowID{ChunkID{2}, 0});
  pos_list_left_1_0->emplace_back(RowID{ChunkID{0}, 1});

  // Left input table, chunk 0, pos_list 1
  auto pos_list_left_0_1 = std::make_shared<PosList>();
  pos_list_left_0_1->emplace_back(RowID{ChunkID{2}, 0});
  pos_list_left_0_1->emplace_back(RowID{ChunkID{1}, 1});
  pos_list_left_0_1->emplace_back(RowID{ChunkID{1}, 1});

  // Left input table, chunk 1, pos_list 1
  auto pos_list_left_1_1 = std::make_shared<PosList>();
  pos_list_left_1_1->emplace_back(RowID{ChunkID{1}, 0});
  pos_list_left_1_1->emplace_back(RowID{ChunkID{2}, 0});

  // Right input table, chunk 0, pos_list 0
  auto pos_list_right_0_0 = std::make_shared<PosList>();
  pos_list_right_0_0->emplace_back(RowID{ChunkID{2}, 0});
  pos_list_right_0_0->emplace_back(RowID{ChunkID{2}, 0});
  pos_list_right_0_0->emplace_back(RowID{ChunkID{1}, 2});
  pos_list_right_0_0->emplace_back(RowID{ChunkID{1}, 0});

  // Right input table, chunk 1, pos_list 0
  auto pos_list_right_1_0 = std::make_shared<PosList>();
  pos_list_right_1_0->emplace_back(RowID{ChunkID{0}, 0});
  pos_list_right_1_0->emplace_back(RowID{ChunkID{2}, 0});

  // Right input table, chunk 0, pos_list 1
  auto pos_list_right_0_1 = std::make_shared<PosList>();
  pos_list_right_0_1->emplace_back(RowID{ChunkID{1}, 0});
  pos_list_right_0_1->emplace_back(RowID{ChunkID{1}, 0});
  pos_list_right_0_1->emplace_back(RowID{ChunkID{2}, 0});
  pos_list_right_0_1->emplace_back(RowID{ChunkID{0}, 0});

  // Right input table, chunk 1, pos_list 1
  auto pos_list_right_1_1 = std::make_shared<PosList>();
  pos_list_right_1_1->emplace_back(RowID{ChunkID{1}, 0});
  pos_list_right_1_1->emplace_back(RowID{ChunkID{1}, 0});

  auto column_left_0_0 = std::make_shared<ReferenceColumn>(_table_int_float4, ColumnID{0}, pos_list_left_0_0);
  auto column_left_1_0 = std::make_shared<ReferenceColumn>(_table_int_float4, ColumnID{0}, pos_list_left_1_0);
  auto column_left_0_1 = std::make_shared<ReferenceColumn>(_table_int_float4, ColumnID{1}, pos_list_left_0_0);
  auto column_left_1_1 = std::make_shared<ReferenceColumn>(_table_int_float4, ColumnID{1}, pos_list_left_1_0);
  auto column_left_0_2 = std::make_shared<ReferenceColumn>(_table_10_ints, ColumnID{0}, pos_list_left_0_1);
  auto column_left_1_2 = std::make_shared<ReferenceColumn>(_table_10_ints, ColumnID{0}, pos_list_left_1_1);

  auto column_right_0_0 = std::make_shared<ReferenceColumn>(_table_int_float4, ColumnID{0}, pos_list_right_0_0);
  auto column_right_1_0 = std::make_shared<ReferenceColumn>(_table_int_float4, ColumnID{0}, pos_list_right_1_0);
  auto column_right_0_1 = std::make_shared<ReferenceColumn>(_table_int_float4, ColumnID{1}, pos_list_right_0_0);
  auto column_right_1_1 = std::make_shared<ReferenceColumn>(_table_int_float4, ColumnID{1}, pos_list_right_1_0);
  auto column_right_0_2 = std::make_shared<ReferenceColumn>(_table_10_ints, ColumnID{0}, pos_list_right_0_1);
  auto column_right_1_2 = std::make_shared<ReferenceColumn>(_table_10_ints, ColumnID{0}, pos_list_right_1_1);

  auto table_left = std::make_shared<Table>(3);
  table_left->add_column_definition("a", DataType::Int);
  table_left->add_column_definition("b", DataType::Float);
  table_left->add_column_definition("c", DataType::Int);

  Chunk chunk_left_0;
  chunk_left_0.add_column(column_left_0_0);
  chunk_left_0.add_column(column_left_0_1);
  chunk_left_0.add_column(column_left_0_2);
  table_left->emplace_chunk(std::move(chunk_left_0));

  Chunk chunk_left_1;
  chunk_left_1.add_column(column_left_1_0);
  chunk_left_1.add_column(column_left_1_1);
  chunk_left_1.add_column(column_left_1_2);
  table_left->emplace_chunk(std::move(chunk_left_1));

  auto table_right = std::make_shared<Table>(4);
  table_right->add_column_definition("a", DataType::Int);
  table_right->add_column_definition("b", DataType::Float);
  table_right->add_column_definition("c", DataType::Int);

  Chunk chunk_right_0;
  chunk_right_0.add_column(column_right_0_0);
  chunk_right_0.add_column(column_right_0_1);
  chunk_right_0.add_column(column_right_0_2);
  table_right->emplace_chunk(std::move(chunk_right_0));

  Chunk chunk_right_1;
  chunk_right_1.add_column(column_right_1_0);
  chunk_right_1.add_column(column_right_1_1);
  chunk_right_1.add_column(column_right_1_2);
  table_right->emplace_chunk(std::move(chunk_right_1));

  auto table_wrapper_left_op = std::make_shared<TableWrapper>(table_left);
  auto table_wrapper_right_op = std::make_shared<TableWrapper>(table_right);
  auto set_union_op = std::make_shared<UnionPositions>(table_wrapper_left_op, table_wrapper_right_op);

  _execute_all({table_wrapper_left_op, table_wrapper_right_op, set_union_op});

  EXPECT_TABLE_EQ_UNORDERED(
      set_union_op->get_output(),
      load_table("src/test/tables/union_positions_multiple_shuffled_pos_list.tbl", Chunk::MAX_SIZE));
}

}  // namespace opossum
