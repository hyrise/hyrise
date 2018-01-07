#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/base_column.hpp"
#include "../lib/storage/chunk.hpp"
#include "../lib/types.hpp"

namespace opossum {

class StorageChunkTest : public BaseTest {
 protected:
  void SetUp() override {
    vc_int = make_shared_by_data_type<BaseColumn, ValueColumn>(DataType::Int);
    vc_int->append(4);
    vc_int->append(6);
    vc_int->append(3);

    vc_str = make_shared_by_data_type<BaseColumn, ValueColumn>(DataType::String);
    vc_str->append("Hello,");
    vc_str->append("world");
    vc_str->append("!");

    c = std::make_shared<Chunk>();
  }

  std::shared_ptr<Chunk> c;
  std::shared_ptr<BaseColumn> vc_int = nullptr;
  std::shared_ptr<BaseColumn> vc_str = nullptr;
};

TEST_F(StorageChunkTest, AddColumnToChunk) {
  EXPECT_EQ(c->size(), 0u);
  c->add_column(vc_int);
  c->add_column(vc_str);
  EXPECT_EQ(c->size(), 3u);
}

TEST_F(StorageChunkTest, AddValuesToChunk) {
  c->add_column(vc_int);
  c->add_column(vc_str);
  c->append({2, "two"});
  EXPECT_EQ(c->size(), 4u);

  if (IS_DEBUG) {
    EXPECT_THROW(c->append({}), std::exception);
    EXPECT_THROW(c->append({4, "val", 3}), std::exception);
    EXPECT_EQ(c->size(), 4u);
  }
}

TEST_F(StorageChunkTest, RetrieveColumn) {
  c->add_column(vc_int);
  c->add_column(vc_str);
  c->append({2, "two"});

  auto base_col = c->get_column(ColumnID{0});
  EXPECT_EQ(base_col->size(), 4u);
}

TEST_F(StorageChunkTest, UnknownColumnType) {
  // Exception will only be thrown in debug builds
  if (IS_DEBUG) {
    auto wrapper = []() { make_shared_by_data_type<BaseColumn, ValueColumn>(DataType::Null); };
    EXPECT_THROW(wrapper(), std::logic_error);
  }
}

}  // namespace opossum
