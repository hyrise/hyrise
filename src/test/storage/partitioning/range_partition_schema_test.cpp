#include "../../base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class StorageRangePartitionSchemaTest : public BaseTest {
 protected:
  void SetUp() override {
    /*
     * Creating a Table partitioned by range on column 0
     * splitting the Table in 3 Partitions.
     * Partition 1: Values <= 5
     * Partition 2: 5 < Values <= 10
     * Partition 3: 10 < Values
     */
    t0.create_range_partitioning(ColumnID{0}, {5, 10});
    t0.add_column("int_column", opossum::DataType::Int, false);
    t0.add_column("string_column", opossum::DataType::String, false);
  }

  Table t0{2};
};

TEST_F(StorageRangePartitionSchemaTest, CreateRangePartitioning) {
  // Three Chunks (one for each Partition) with 0 rows (nothing inserted yet).
  EXPECT_EQ(t0.row_count(), 0u);
  EXPECT_EQ(t0.chunk_count(), 3u);
  EXPECT_TRUE(t0.is_partitioned());
}

TEST_F(StorageRangePartitionSchemaTest, AppendViaTable) {
  // Three in first partition --> two chunks
  t0.append({3, "Foo"});
  t0.append({4, "Bar"});
  t0.append({5, "Baz"});

  // Two in second partition --> one chunks
  t0.append({6, "Foo"});
  t0.append({10, "Bar"});

  // One in third partition --> one chunk
  t0.append({11, "Baz"});

  // First Partition now holds two Chunks. Second and Third only one.
  EXPECT_EQ(t0.chunk_count(), 4u);
}

#if IS_DEBUG

TEST_F(StorageRangePartitionSchemaTest, AppendDirectly) {
  t0.get_modifiable_partition_schema()->append({1, "Foo"});
  t0.get_modifiable_partition_schema()->append({2, "Bar"});

  t0.get_modifiable_partition_schema()->append({6, "Baz"});

  EXPECT_EQ(t0.row_count(), 3u);
  EXPECT_EQ(t0.chunk_count(), 3u);
  EXPECT_EQ(t0.get_chunk(ChunkID{0})->size(), 2u);
  EXPECT_EQ(t0.get_chunk(ChunkID{1})->size(), 1u);
  EXPECT_EQ(t0.get_chunk(ChunkID{2})->size(), 0u);
}

TEST_F(StorageRangePartitionSchemaTest, AppendDirectlyCanExceedMaxChunkSize) {
  t0.get_modifiable_partition_schema()->append({1, "Foo"});
  t0.get_modifiable_partition_schema()->append({2, "Bar"});
  t0.get_modifiable_partition_schema()->append({3, "Baz"});

  // No new chunk is created since this is done by Table which is not involved here.
  EXPECT_EQ(t0.row_count(), 3u);
  EXPECT_EQ(t0.chunk_count(), 3u);
  EXPECT_EQ(t0.get_chunk(ChunkID{0})->size(), 3u);
}

#endif

TEST_F(StorageRangePartitionSchemaTest, Name) { EXPECT_EQ(t0.get_partition_schema()->name(), "RangePartition"); }

}  // namespace opossum
