#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/partitioning/range_partition_schema.hpp"

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
    std::vector<AllTypeVariant> bounds = {5, 10};
    t0.apply_partitioning(std::make_shared<RangePartitionSchema>(ColumnID{0}, bounds));
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

TEST_F(StorageRangePartitionSchemaTest, Name) { EXPECT_EQ(t0.get_partition_schema()->name(), "RangePartition"); }

TEST_F(StorageRangePartitionSchemaTest, GetColumnID) {
  EXPECT_EQ(std::dynamic_pointer_cast<const RangePartitionSchema>(t0.get_partition_schema())->get_column_id(),
            ColumnID{0});
}

TEST_F(StorageRangePartitionSchemaTest, GetChunkIDsToExclude) {
  const auto chunk_ids = t0.get_partition_schema()->get_chunk_ids_to_exclude(PredicateCondition::LessThan, AllTypeVariant{4});
  EXPECT_EQ(chunk_ids.size(), 2u);
}

}  // namespace opossum
