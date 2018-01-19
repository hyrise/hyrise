#include "../../base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class StorageRangePartitionSchemaTest : public BaseTest {
 protected:
  void SetUp() override {
    t0.create_range_partitioning(ColumnID{0}, {5, 10});
    t0.add_column("int_column", opossum::DataType::Int, false);
    t0.add_column("string_column", opossum::DataType::String, false);
  }

  Table t0{2};
};

TEST_F(StorageRangePartitionSchemaTest, CreateRangePartitioning) {
  EXPECT_EQ(t0.row_count(), 0u);
  EXPECT_EQ(t0.chunk_count(), 3u);
  EXPECT_TRUE(t0.is_partitioned());
}

TEST_F(StorageRangePartitionSchemaTest, AppendViaTable) {
  // three in first partition --> two chunks
  t0.append({1, "Foo"});
  t0.append({2, "Bar"});
  t0.append({3, "Baz"});

  // two in second partition --> one chunks
  t0.append({6, "Foo"});
  t0.append({7, "Bar"});

  // one in third partition --> one chunk
  t0.append({11, "Baz"});

  EXPECT_EQ(t0.chunk_count(), 4u);
}

TEST_F(StorageRangePartitionSchemaTest, AppendDirectly) {
  t0.get_modifiable_partition_schema()->append({1, "Foo"});
  t0.get_modifiable_partition_schema()->append({2, "Bar"});

  t0.get_modifiable_partition_schema()->append({6, "Baz"});

  EXPECT_EQ(t0.row_count(), 3u);
  EXPECT_EQ(t0.chunk_count(), 3u);
  EXPECT_EQ(t0.get_chunk(ChunkID{0}).size(), 2u);
  EXPECT_EQ(t0.get_chunk(ChunkID{1}).size(), 1u);
  EXPECT_EQ(t0.get_chunk(ChunkID{2}).size(), 0u);
}

TEST_F(StorageRangePartitionSchemaTest, AppendDirectlyCanExceedMaxChunkSize) {
  t0.get_modifiable_partition_schema()->append({1, "Foo"});
  t0.get_modifiable_partition_schema()->append({2, "Bar"});
  t0.get_modifiable_partition_schema()->append({3, "Baz"});

  EXPECT_EQ(t0.row_count(), 3u);
  EXPECT_EQ(t0.chunk_count(), 3u);
  EXPECT_EQ(t0.get_chunk(ChunkID{0}).size(), 3u);
}

TEST_F(StorageRangePartitionSchemaTest, Name) { EXPECT_EQ(t0.get_partition_schema()->name(), "RangePartition"); }

}  // namespace opossum
