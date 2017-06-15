#include <limits>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/dictionary_column.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  Table t{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  t.get_chunk(ChunkID{0});
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.get_chunk(ChunkID{q}), std::exception);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.get_chunk(ChunkID{1});
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

TEST_F(StorageTableTest, GetValue) {
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  ASSERT_EQ(t.get_value<int>(0u, 0u), 4);
  EXPECT_EQ(t.get_value<int>(0u, 2u), 3);
  ASSERT_FALSE(t.get_value<std::string>(1u, 0u).compare("Hello,"));
  ASSERT_FALSE(t.get_value<std::string>(1u, 2u).compare("!"));
  EXPECT_THROW(t.get_value<int>(3u, 0u), std::exception);
}

TEST_F(StorageTableTest, ColumnNameTooLong) {
  EXPECT_THROW(t.add_column(std::string(std::numeric_limits<ColumnNameLength>::max() + 1ul, 'A'), "int");
               , std::exception);
}

TEST_F(StorageTableTest, ShrinkingMvccColumnsHasNoSideEffects) {
  t.append({4, "Hello,"});
  t.append({6, "world"});

  auto& chunk = t.get_chunk(ChunkID{0});

  const auto values = std::vector<CommitID>{1u, 2u};

  {
    // acquiring mvcc_columns locks them
    auto mvcc_columns = chunk.mvcc_columns();

    mvcc_columns->tids[0u] = values[0u];
    mvcc_columns->tids[1u] = values[1u];
    mvcc_columns->begin_cids[0u] = values[0u];
    mvcc_columns->begin_cids[1u] = values[1u];
    mvcc_columns->end_cids[0u] = values[0u];
    mvcc_columns->end_cids[1u] = values[1u];
  }

  const auto previous_size = chunk.size();

  chunk.shrink_mvcc_columns();

  ASSERT_EQ(previous_size, chunk.size());
  ASSERT_TRUE(chunk.has_mvcc_columns());

  auto new_mvcc_columns = chunk.mvcc_columns();

  for (auto i = 0u; i < chunk.size(); ++i) {
    EXPECT_EQ(new_mvcc_columns->tids[i], values[i]);
    EXPECT_EQ(new_mvcc_columns->begin_cids[i], values[i]);
    EXPECT_EQ(new_mvcc_columns->end_cids[i], values[i]);
  }
}

}  // namespace opossum
