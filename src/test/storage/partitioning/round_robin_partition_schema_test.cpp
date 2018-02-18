#include "../../base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class StorageRoundRobinPartitionSchemaTest : public BaseTest {
 protected:
  void SetUp() override {
    // Creating a Table with round robin partitioning using two Partitions.
    t0.create_round_robin_partitioning(PartitionID{2});
    t0.add_column("int_column", opossum::DataType::Int, false);
    t0.add_column("string_column", opossum::DataType::String, false);
  }

  Table t0{2};
};

TEST_F(StorageRoundRobinPartitionSchemaTest, CreateRangePartitioning) {
  // Two Chunks (one for each Partition) with 0 rows (nothing inserted yet).
  EXPECT_EQ(t0.row_count(), 0u);
  EXPECT_EQ(t0.chunk_count(), 2u);
  EXPECT_TRUE(t0.is_partitioned());
}

TEST_F(StorageRoundRobinPartitionSchemaTest, AppendViaTable) {
  t0.append({1, "Foo"});  // --> Chunk 0, first Chunk in Partition 1
  t0.append({2, "Bar"});  // --> Chunk 1, first Chunk in Partition 2
  t0.append({3, "Baz"});  // --> Chunk 0, first Chunk in Partition 1
  t0.append({4, "Foo"});  // --> Chunk 1, first Chunk in Partition 2
  t0.append({5, "Bar"});  // --> Chunk 2, second Chunk in Partition 1

  EXPECT_EQ(t0.row_count(), 5u);
  EXPECT_EQ(t0.chunk_count(), 3u);
  EXPECT_EQ(t0.get_chunk(ChunkID{0})->size(), 2u);
  EXPECT_EQ(t0.get_chunk(ChunkID{1})->size(), 2u);
  EXPECT_EQ(t0.get_chunk(ChunkID{2})->size(), 1u);
}

#if IS_DEBUG

TEST_F(StorageRoundRobinPartitionSchemaTest, AppendDirectly) {
  t0.get_mutable_partition_schema()->append({1, "Foo"});
  t0.get_mutable_partition_schema()->append({2, "Bar"});
  t0.get_mutable_partition_schema()->append({3, "Baz"});

  EXPECT_EQ(t0.row_count(), 3u);
  EXPECT_EQ(t0.chunk_count(), 2u);
  EXPECT_EQ(t0.get_chunk(ChunkID{0})->size(), 2u);
  EXPECT_EQ(t0.get_chunk(ChunkID{1})->size(), 1u);
}

TEST_F(StorageRoundRobinPartitionSchemaTest, AppendDirectlyCanExceedMaxChunkSize) {
  t0.get_mutable_partition_schema()->append({1, "Foo"});
  t0.get_mutable_partition_schema()->append({2, "Bar"});
  t0.get_mutable_partition_schema()->append({3, "Baz"});
  t0.get_mutable_partition_schema()->append({4, "Foo"});
  t0.get_mutable_partition_schema()->append({5, "Bar"});

  // No new chunk is created since this is done by Table which is not involved here.
  EXPECT_EQ(t0.row_count(), 5u);
  EXPECT_EQ(t0.chunk_count(), 2u);
  EXPECT_EQ(t0.get_chunk(ChunkID{0})->size(), 3u);
  EXPECT_EQ(t0.get_chunk(ChunkID{1})->size(), 2u);
}

#endif

TEST_F(StorageRoundRobinPartitionSchemaTest, Name) {
  EXPECT_EQ(t0.get_partition_schema()->name(), "RoundRobinPartition");
}

}  // namespace opossum
