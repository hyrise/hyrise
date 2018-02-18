#include "../../base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class StorageNullPartitionSchemaTest : public BaseTest {
 protected:
  void SetUp() override {
    t0.add_column("int_column", opossum::DataType::Int, false);
    t0.add_column("string_column", opossum::DataType::String, false);
  }

  Table t0{2};
};

/*
 * In this test we make sure that NullPartitionSchema is the default PartitionSchema.
 * The behavior tested is the same as in the Table tests.
 */

TEST_F(StorageNullPartitionSchemaTest, NullPartitioningIsDefault) {
  EXPECT_EQ(t0.row_count(), 0u);
  EXPECT_EQ(t0.chunk_count(), 1u);
  EXPECT_FALSE(t0.is_partitioned());
}

TEST_F(StorageNullPartitionSchemaTest, AppendViaTable) {
  t0.append({1, "Foo"});
  t0.append({2, "Bar"});
  t0.append({3, "Baz"});
  t0.append({6, "Foo"});
  t0.append({7, "Bar"});
  t0.append({11, "Baz"});

  EXPECT_EQ(t0.chunk_count(), 3u);
}

#if IS_DEBUG

TEST_F(StorageNullPartitionSchemaTest, AppendDirectly) {
  t0.get_mutable_partition_schema()->append({1, "Foo"});
  t0.get_mutable_partition_schema()->append({2, "Bar"});

  EXPECT_EQ(t0.row_count(), 2u);
  EXPECT_EQ(t0.chunk_count(), 1u);
  EXPECT_EQ(t0.get_chunk(ChunkID{0})->size(), 2u);
}

TEST_F(StorageNullPartitionSchemaTest, AppendDirectlyCanExceedMaxChunkSize) {
  t0.get_mutable_partition_schema()->append({1, "Foo"});
  t0.get_mutable_partition_schema()->append({2, "Bar"});
  t0.get_mutable_partition_schema()->append({3, "Baz"});

  // No new chunk is created since this is done by Table which is not involved here.
  EXPECT_EQ(t0.row_count(), 3u);
  EXPECT_EQ(t0.chunk_count(), 1u);
  EXPECT_EQ(t0.get_chunk(ChunkID{0})->size(), 3u);
}

#endif

TEST_F(StorageNullPartitionSchemaTest, Name) { EXPECT_EQ(t0.get_partition_schema()->name(), "NullPartition"); }

}  // namespace opossum
