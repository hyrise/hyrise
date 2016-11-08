#include "gtest/gtest.h"

#include "../../lib/storage/table.hpp"

class StorageTableTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  opossum::Table t{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 7u);
}

TEST_F(StorageTableTest, GetChunk) {
  t.get_chunk(0);
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.get_chunk(1), std::exception);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.get_chunk(1);
}

TEST_F(StorageTableTest, ColCount) { EXPECT_EQ(t.col_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(0), "col_1");
  EXPECT_EQ(t.column_name(1), "col_2");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_name(2), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(0), "int");
  EXPECT_EQ(t.column_type(1), "string");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_type(2), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.chunk_size(), 2u); }
