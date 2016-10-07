#include <memory>

#include "gtest/gtest.h"

#include "../../lib/common.hpp"
#include "../../lib/storage/base_column.hpp"
#include "../../lib/storage/chunk.hpp"
#include "../../lib/types.hpp"

class StorageChunkTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    vc_int = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::ValueColumn>("int");
    vc_int->append(4);
    vc_int->append(6);
    vc_int->append(3);

    vc_str = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::ValueColumn>("string");
    vc_str->append("Hello,");
    vc_str->append("world");
    vc_str->append("!");
  }

  opossum::Chunk c;
  std::shared_ptr<opossum::BaseColumn> vc_int = nullptr;
  std::shared_ptr<opossum::BaseColumn> vc_str = nullptr;
};

TEST_F(StorageChunkTest, AddColumnToChunk) {
  EXPECT_EQ(c.size(), 0u);
  c.add_column(vc_int);
  c.add_column(vc_str);
  EXPECT_EQ(c.size(), 3u);
}

TEST_F(StorageChunkTest, AddValuesToChunk) {
  c.add_column(vc_int);
  c.add_column(vc_str);
  c.append({2, "two"});
  EXPECT_EQ(c.size(), 4u);

  EXPECT_THROW(c.append({}), std::exception);
  EXPECT_THROW(c.append({4, "val", 3}), std::exception);
  EXPECT_EQ(c.size(), 4u);
}

TEST_F(StorageChunkTest, RetrieveColumn) {
  c.add_column(vc_int);
  c.add_column(vc_str);
  c.append({2, "two"});

  auto base_col = c.get_column(0);
  EXPECT_EQ(base_col->size(), 4u);
}
