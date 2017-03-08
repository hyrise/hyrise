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

TEST_F(StorageTableTest, ChunkCompression) {
  Table t2{6, true};
  t2.add_column("col_1", "int");
  t2.add_column("col_2", "string");
  t2.append({4, "Bill"});
  t2.append({4, "Steve"});
  t2.append({3, "Alexander"});
  t2.append({4, "Steve"});
  t2.append({5, "Hasso"});
  t2.append({3, "Alexander"});
  t2.append({1, "Bill"});

  EXPECT_EQ(t2.chunk_count(), 2u);
  // Test attribute vectors
  EXPECT_EQ(t2.get_chunk(0).get_column(0)->size(), 6u);
  EXPECT_EQ(t2.get_chunk(0).get_column(1)->size(), 6u);

  // Test dictionary size
  auto col_1 = std::dynamic_pointer_cast<DictionaryColumn<int>>(t2.get_chunk(0).get_column(0));
  auto col_2 = std::dynamic_pointer_cast<DictionaryColumn<std::string>>(t2.get_chunk(0).get_column(1));

  // Test if ValueColumn has been transformed to DictionaryColumn
  EXPECT_NE(col_1, nullptr);
  EXPECT_NE(col_2, nullptr);
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

TEST_F(StorageTableTest, ColumnNameTooLong) {
  EXPECT_THROW(t.add_column(std::string(std::numeric_limits<ColumnNameLength>::max() + 1ul, 'A'), "int");
               , std::exception);
}

TEST_F(StorageTableTest, CompressedChunkHasSameCidColumns) {
  t.append({4, "Hello,"});
  t.append({6, "world"});

  auto& chunk = t.get_chunk(0u);

  const auto values = std::vector<CommitID>{1u, 2u};

  auto& mvcc_columns = chunk.mvcc_columns();

  // tids are not copied because they must be 0, since
  // otherwise someone else would be trying to
  // simultaneously change the records
  mvcc_columns.begin_cids[0u] = values[0u];
  mvcc_columns.begin_cids[1u] = values[1u];
  mvcc_columns.end_cids[0u] = values[0u];
  mvcc_columns.end_cids[1u] = values[1u];

  const auto previous_size = chunk.size();

  t.compress_chunk(0u);

  auto& compressed_chunk = t.get_chunk(0u);

  ASSERT_EQ(previous_size, compressed_chunk.size());
  ASSERT_TRUE(compressed_chunk.has_mvcc_columns());

  const auto& new_mvcc_columns = compressed_chunk.mvcc_columns();

  for (auto i = 0u; i < chunk.size(); ++i) {
    EXPECT_EQ(new_mvcc_columns.begin_cids[i], values[i]);
    EXPECT_EQ(new_mvcc_columns.end_cids[i], values[i]);
  }
}

}  // namespace opossum
