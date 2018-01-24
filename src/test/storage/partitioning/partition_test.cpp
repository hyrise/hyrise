
#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/base_column.hpp"
#include "../lib/storage/chunk.hpp"
#include "../lib/storage/partitioning/partition.hpp"
#include "../lib/storage/value_column.hpp"
#include "../lib/types.hpp"

namespace opossum {

class StoragePartitionTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(StoragePartitionTest, CreateAndGetId) {
  Partition p0{PartitionID{0}};
  Partition p1{PartitionID{1}};

  EXPECT_EQ(p0.get_partition_id(), PartitionID{0});
  EXPECT_EQ(p1.get_partition_id(), PartitionID{1});
}

TEST_F(StoragePartitionTest, AddAndAccessChunk) {
  Partition p0{PartitionID{0}};
  std::shared_ptr<Chunk> c0 = std::make_shared<Chunk>();
  std::shared_ptr<Chunk> c1 = std::make_shared<Chunk>();

  p0.add_new_chunk(c0);
  EXPECT_EQ(p0.last_chunk(), c0);

  p0.add_new_chunk(c1);
  EXPECT_EQ(p0.last_chunk(), c1);
}

TEST_F(StoragePartitionTest, Append) {
  Partition p0{PartitionID{0}};
  std::shared_ptr<Chunk> c0 = std::make_shared<Chunk>();
  std::shared_ptr<BaseColumn> vc_int = make_shared_by_data_type<BaseColumn, ValueColumn>(DataType::Int);
  std::shared_ptr<BaseColumn> vc_str = make_shared_by_data_type<BaseColumn, ValueColumn>(DataType::String);

  // Chunks have to be pre-configured when adding them to a Partition.
  // This is usually done by Table. Here we have to do this manually.
  c0->add_column(vc_int);
  c0->add_column(vc_str);

  p0.add_new_chunk(c0);
  p0.append({21, "Hello"});
  p0.append({42, "World"});

  EXPECT_EQ(c0->size(), 2u);
  EXPECT_EQ(c0->get_column(ColumnID{0}), vc_int);
  EXPECT_EQ(c0->get_column(ColumnID{1}), vc_str);
}

}  // namespace opossum
