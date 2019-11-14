#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/table.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class LoadTableTest : public BaseTest {};

TEST_F(LoadTableTest, EmptyTableFromHeader) {
  const auto tbl_header_table = create_table_from_header("resources/test_data/tbl/float_int.tbl");
  const auto expected_table = std::make_shared<Table>(
      TableColumnDefinitions{{"b", DataType::Float, false}, {"a", DataType::Int, false}}, TableType::Data);

  EXPECT_EQ(tbl_header_table->row_count(), 0);
  EXPECT_TABLE_EQ_UNORDERED(tbl_header_table, expected_table);
}

TEST_F(LoadTableTest, AllChunksFinalized) {
  const auto table = load_table("resources/test_data/tbl/float_int.tbl", 2);

  EXPECT_EQ(table->row_count(), 3);
  EXPECT_EQ(table->chunk_count(), 2);

  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    // During finalization, chunks are set to immutable
    EXPECT_FALSE(table->get_chunk(chunk_id)->is_mutable());
  }
}

}  // namespace opossum
