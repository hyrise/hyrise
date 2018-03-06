#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/partitioning/round_robin_partition_schema.hpp"

namespace opossum {

class StorageRoundRobinPartitionSchemaTest : public BaseTest {
 protected:
  void SetUp() override {
    // Creating a Table with round robin partitioning using two Partitions.
    t0.apply_partitioning(std::make_shared<RoundRobinPartitionSchema>(PartitionID{2}));
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

TEST_F(StorageRoundRobinPartitionSchemaTest, Name) {
  EXPECT_EQ(t0.get_partition_schema()->name(), "RoundRobinPartition");
}

TEST_F(StorageRoundRobinPartitionSchemaTest, GetChunkIDsToExclude) {
  const auto chunk_ids = t0.get_partition_schema()->get_chunk_ids_to_exclude(PredicateCondition::Equals, AllTypeVariant{2});
  EXPECT_EQ(chunk_ids.size(), 0u);
}

}  // namespace opossum
