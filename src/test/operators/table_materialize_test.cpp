#include "base_test.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_materialize.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"
#include "storage/chunk_encoder.hpp"

namespace opossum {

class TableMaterializeTest : public BaseTest {
 public:
  void SetUp() override {
    table = load_table("resources/test_data/tbl/int_float.tbl", 2);

    table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<TableWrapper> table_wrapper;
};

TEST_F(TableMaterializeTest, MaterializeEncodedTable) {
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  const auto table_materialize = std::make_shared<TableMaterialize>(table_wrapper);
  table_materialize->execute();

  const auto materialized_table = table_materialize->get_output();

  EXPECT_TABLE_EQ_ORDERED(materialized_table, table);

  ASSERT_EQ(materialized_table->chunk_count(), 1u);
  EXPECT_TRUE(std::dynamic_pointer_cast<ValueSegment<int32_t>>(materialized_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0})));
  EXPECT_TRUE(std::dynamic_pointer_cast<ValueSegment<float>>(materialized_table->get_chunk(ChunkID{0})->get_segment(ColumnID{1})));
}

TEST_F(TableMaterializeTest, MaterializeReferenceTable) {
  const auto table_scan = create_table_scan(table_wrapper, ColumnID{0}, PredicateCondition::NotEquals, 123);
  table_scan->execute();

  const auto table_materialize = std::make_shared<TableMaterialize>(table_scan);
  table_materialize->execute();

  const auto materialized_table = table_materialize->get_output();

  EXPECT_TABLE_EQ_UNORDERED(materialized_table, load_table("resources/test_data/tbl/int_float_filtered2.tbl"));

  ASSERT_EQ(materialized_table->chunk_count(), 1u);
  EXPECT_TRUE(std::dynamic_pointer_cast<ValueSegment<int32_t>>(materialized_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0})));
  EXPECT_TRUE(std::dynamic_pointer_cast<ValueSegment<float>>(materialized_table->get_chunk(ChunkID{0})->get_segment(ColumnID{1})));
}

TEST_F(TableMaterializeTest, MaterializeMaterializedTable) {
  // Sanity Test - Materializing an already materialized table should work

  const auto table_materialize = std::make_shared<TableMaterialize>(table_wrapper);
  table_materialize->execute();

  const auto materialized_table = table_materialize->get_output();

  EXPECT_TABLE_EQ_ORDERED(table, materialized_table);

  ASSERT_EQ(materialized_table->chunk_count(), 1u);
  EXPECT_TRUE(std::dynamic_pointer_cast<ValueSegment<int32_t>>(materialized_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0})));
  EXPECT_TRUE(std::dynamic_pointer_cast<ValueSegment<float>>(materialized_table->get_chunk(ChunkID{0})->get_segment(ColumnID{1})));
}

}  // namespace opossum
