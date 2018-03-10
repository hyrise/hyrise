#include "gtest/gtest.h"

#include "storage/defragment_reference_table.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class DefragmentReferenceTableTest : public ::testing::Test {
 public:
  void SetUp() override {
    _input_table_a = load_table("src/test/tables/int_int3.tbl", 3);
    _input_table_b = load_table("src/test/tables/int_int3.tbl", 3);
  }

  std::shared_ptr<Table> _input_table_a, _input_table_b;
};

TEST_F(DefragmentReferenceTableTest, Simple) {
  /**
   * Test table with 5 chunks. The first 3 can be merged, in the 4th one column points to a different table and the
   * fifth hast too many rows to be merged with the fourth
   */

  const auto pos_list_a = std::make_shared<PosList>(
      PosList{{{ChunkID{0}, ChunkOffset{0}}, {ChunkID{1}, ChunkOffset{1}}, {ChunkID{1}, ChunkOffset{0}}}});
  const auto pos_list_b = std::make_shared<PosList>(PosList{{
      {ChunkID{1}, ChunkOffset{1}}, {ChunkID{1}, ChunkOffset{1}},
  }});
  const auto pos_list_c = std::make_shared<PosList>(
      PosList{{{ChunkID{0}, ChunkOffset{2}}, {ChunkID{1}, ChunkOffset{1}}, {ChunkID{2}, ChunkOffset{0}}}});
  const auto pos_list_d = std::make_shared<PosList>(PosList{{{ChunkID{0}, ChunkOffset{2}},
                                                             {ChunkID{1}, ChunkOffset{1}},
                                                             {ChunkID{2}, ChunkOffset{0}},
                                                             {ChunkID{0}, ChunkOffset{2}},
                                                             {ChunkID{1}, ChunkOffset{1}},
                                                             {ChunkID{2}, ChunkOffset{0}},
                                                             {ChunkID{0}, ChunkOffset{2}},
                                                             {ChunkID{1}, ChunkOffset{1}},
                                                             {ChunkID{2}, ChunkOffset{0}}}});

  const auto column_definitions =
      TableColumnDefinitions{{"a", DataType::Int}, {"b", DataType::Int}, {"c", DataType::Int}};

  const auto reference_table = std::make_shared<Table>(column_definitions, TableType::References);

  reference_table->append_chunk({
      std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{0}, pos_list_a),
      std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{1}, pos_list_a),
      std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{0}, pos_list_a),
  });
  reference_table->append_chunk({std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{0}, pos_list_b),
                                 std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{1}, pos_list_b),
                                 std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{0}, pos_list_b)});
  reference_table->append_chunk({
      std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{0}, pos_list_c),
      std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{1}, pos_list_c),
      std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{0}, pos_list_a),
  });
  // Chunk has the same PosList as Chunk above - shouldn't happen in reality, but neither should it confuse the
  // algorithm
  reference_table->append_chunk({std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{0}, pos_list_c),
                                 std::make_shared<ReferenceColumn>(_input_table_b, ColumnID{1}, pos_list_c),
                                 std::make_shared<ReferenceColumn>(_input_table_b, ColumnID{0}, pos_list_c)});
  reference_table->append_chunk({std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{0}, pos_list_d),
                                 std::make_shared<ReferenceColumn>(_input_table_a, ColumnID{1}, pos_list_d),
                                 std::make_shared<ReferenceColumn>(_input_table_b, ColumnID{0}, pos_list_d)});

  const auto defragmented_table = defragment_reference_table(reference_table, ChunkOffset{4}, ChunkOffset{8});

  EXPECT_TABLE_EQ_ORDERED(reference_table, defragmented_table);

  ASSERT_EQ(defragmented_table->chunk_count(), 3u);
  ASSERT_EQ(defragmented_table->column_count(), 3u);
  ASSERT_EQ(defragmented_table->get_chunk(ChunkID(0))->size(), 8u);
  ASSERT_EQ(defragmented_table->get_chunk(ChunkID(1))->size(), 3u);
  ASSERT_EQ(defragmented_table->get_chunk(ChunkID(2))->size(), 9u);

  /**
   * Test that the first two columns of the first Chunk use the same pos list
   */
  const auto chunk_a = defragmented_table->get_chunk(ChunkID(0));
  const auto reference_column_a = std::dynamic_pointer_cast<const ReferenceColumn>(chunk_a->get_column(ColumnID(0)));
  const auto reference_column_b = std::dynamic_pointer_cast<const ReferenceColumn>(chunk_a->get_column(ColumnID(1)));
  const auto reference_column_c = std::dynamic_pointer_cast<const ReferenceColumn>(chunk_a->get_column(ColumnID(2)));

  EXPECT_EQ(reference_column_a->pos_list(), reference_column_b->pos_list());
  EXPECT_NE(reference_column_b->pos_list(), reference_column_c->pos_list());
}

}  // namespace opossum
