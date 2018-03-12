#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/base_column.hpp"
#include "../lib/storage/chunk.hpp"
#include "../lib/storage/column_encoding_utils.hpp"
#include "../lib/storage/index/group_key/composite_group_key_index.hpp"
#include "../lib/storage/index/group_key/group_key_index.hpp"
#include "../lib/types.hpp"

namespace opossum {

class StorageChunkTest : public BaseTest {
 protected:
  void SetUp() override {
    vc_int = make_shared_by_data_type<BaseValueColumn, ValueColumn>(DataType::Int);
    vc_int->append(4);
    vc_int->append(6);
    vc_int->append(3);

    vc_str = make_shared_by_data_type<BaseValueColumn, ValueColumn>(DataType::String);
    vc_str->append("Hello,");
    vc_str->append("world");
    vc_str->append("!");

    dc_int = encode_column(EncodingType::Dictionary, DataType::Int, vc_int);
    dc_str = encode_column(EncodingType::Dictionary, DataType::String, vc_str);

    ChunkColumns empty_columns;
    empty_columns.push_back(std::make_shared<ValueColumn<int32_t>>());
    empty_columns.push_back(std::make_shared<ValueColumn<std::string>>());

    c = std::make_shared<Chunk>(empty_columns);
  }

  std::shared_ptr<Chunk> c;
  std::shared_ptr<BaseValueColumn> vc_int = nullptr;
  std::shared_ptr<BaseValueColumn> vc_str = nullptr;
  std::shared_ptr<BaseColumn> dc_int = nullptr;
  std::shared_ptr<BaseColumn> dc_str = nullptr;
};

TEST_F(StorageChunkTest, AddColumnToChunk) {
  EXPECT_EQ(c->size(), 0u);
  c = std::make_shared<Chunk>(ChunkColumns({vc_int, vc_str}));
  EXPECT_EQ(c->size(), 3u);
}

TEST_F(StorageChunkTest, AddValuesToChunk) {
  c = std::make_shared<Chunk>(ChunkColumns({vc_int, vc_str}));
  c->append({2, "two"});
  EXPECT_EQ(c->size(), 4u);

  if (IS_DEBUG) {
    EXPECT_THROW(c->append({}), std::exception);
    EXPECT_THROW(c->append({4, "val", 3}), std::exception);
    EXPECT_EQ(c->size(), 4u);
  }
}

TEST_F(StorageChunkTest, RetrieveColumn) {
  c = std::make_shared<Chunk>(ChunkColumns({vc_int, vc_str}));
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

TEST_F(StorageChunkTest, AddIndexByColumnID) {
  c = std::make_shared<Chunk>(ChunkColumns({dc_int, dc_str}));
  auto index_int = c->create_index<GroupKeyIndex>(std::vector<ColumnID>{ColumnID{0}});
  auto index_str = c->create_index<GroupKeyIndex>(std::vector<ColumnID>{ColumnID{0}});
  auto index_int_str = c->create_index<CompositeGroupKeyIndex>(std::vector<ColumnID>{ColumnID{0}, ColumnID{1}});
  EXPECT_TRUE(index_int);
  EXPECT_TRUE(index_str);
  EXPECT_TRUE(index_int_str);
}

TEST_F(StorageChunkTest, AddIndexByColumnPointer) {
  c = std::make_shared<Chunk>(ChunkColumns({dc_int, dc_str}));
  auto index_int = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int});
  auto index_str = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_str});
  auto index_int_str =
      c->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int, dc_str});
  EXPECT_TRUE(index_int);
  EXPECT_TRUE(index_str);
  EXPECT_TRUE(index_int_str);
}

TEST_F(StorageChunkTest, GetIndexByColumnID) {
  c = std::make_shared<Chunk>(ChunkColumns({dc_int, dc_str}));
  auto index_int = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int});
  auto index_str = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_str});
  auto index_int_str =
      c->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int, dc_str});

  EXPECT_EQ(c->get_index(ColumnIndexType::GroupKey, std::vector<ColumnID>{ColumnID{0}}), index_int);
  EXPECT_EQ(c->get_index(ColumnIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{0}}), index_int_str);
  EXPECT_EQ(c->get_index(ColumnIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{0}, ColumnID{1}}),
            index_int_str);
  EXPECT_EQ(c->get_index(ColumnIndexType::GroupKey, std::vector<ColumnID>{ColumnID{1}}), index_str);
  EXPECT_EQ(c->get_index(ColumnIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{1}}), nullptr);
}

TEST_F(StorageChunkTest, GetIndexByColumnPointer) {
  c = std::make_shared<Chunk>(ChunkColumns({dc_int, dc_str}));
  auto index_int = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int});
  auto index_str = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_str});
  auto index_int_str =
      c->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int, dc_str});

  EXPECT_EQ(c->get_index(ColumnIndexType::GroupKey, std::vector<std::shared_ptr<const BaseColumn>>{dc_int}), index_int);
  EXPECT_EQ(c->get_index(ColumnIndexType::CompositeGroupKey, std::vector<std::shared_ptr<const BaseColumn>>{dc_int}),
            index_int_str);
  EXPECT_EQ(
      c->get_index(ColumnIndexType::CompositeGroupKey, std::vector<std::shared_ptr<const BaseColumn>>{dc_int, dc_str}),
      index_int_str);
  EXPECT_EQ(c->get_index(ColumnIndexType::GroupKey, std::vector<std::shared_ptr<const BaseColumn>>{dc_str}), index_str);
  EXPECT_EQ(c->get_index(ColumnIndexType::CompositeGroupKey, std::vector<std::shared_ptr<const BaseColumn>>{dc_str}),
            nullptr);
}

TEST_F(StorageChunkTest, GetIndicesByColumnIDs) {
  c = std::make_shared<Chunk>(ChunkColumns({dc_int, dc_str}));
  auto index_int = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int});
  auto index_str = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_str});
  auto index_int_str =
      c->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int, dc_str});

  auto ind_col_0 = c->get_indices(std::vector<ColumnID>{ColumnID{0}});
  // Make sure it finds both the single-column index as well as the multi-column index
  EXPECT_NE(std::find(ind_col_0.cbegin(), ind_col_0.cend(), index_int), ind_col_0.cend());
  EXPECT_NE(std::find(ind_col_0.cbegin(), ind_col_0.cend(), index_int_str), ind_col_0.cend());

  auto ind_col_1 = c->get_indices(std::vector<ColumnID>{ColumnID{1}});
  // Make sure it only finds the single-column index
  EXPECT_NE(std::find(ind_col_1.cbegin(), ind_col_1.cend(), index_str), ind_col_1.cend());
  EXPECT_EQ(std::find(ind_col_1.cbegin(), ind_col_1.cend(), index_int_str), ind_col_1.cend());
}

TEST_F(StorageChunkTest, GetIndicesByColumnPointers) {
  c = std::make_shared<Chunk>(ChunkColumns({dc_int, dc_str}));
  auto index_int = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int});
  auto index_str = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_str});
  auto index_int_str =
      c->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int, dc_str});

  auto ind_col_0 = c->get_indices(std::vector<std::shared_ptr<const BaseColumn>>{dc_int});
  // Make sure it finds both the single-column index as well as the multi-column index
  EXPECT_NE(std::find(ind_col_0.cbegin(), ind_col_0.cend(), index_int), ind_col_0.cend());
  EXPECT_NE(std::find(ind_col_0.cbegin(), ind_col_0.cend(), index_int_str), ind_col_0.cend());

  auto ind_col_1 = c->get_indices(std::vector<std::shared_ptr<const BaseColumn>>{dc_str});
  // Make sure it only finds the single-column index
  EXPECT_NE(std::find(ind_col_1.cbegin(), ind_col_1.cend(), index_str), ind_col_1.cend());
  EXPECT_EQ(std::find(ind_col_1.cbegin(), ind_col_1.cend(), index_int_str), ind_col_1.cend());
}

TEST_F(StorageChunkTest, RemoveIndex) {
  c = std::make_shared<Chunk>(ChunkColumns({dc_int, dc_str}));
  auto index_int = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int});
  auto index_str = c->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_str});
  auto index_int_str =
      c->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dc_int, dc_str});

  c->remove_index(index_int);
  auto ind_col_0 = c->get_indices(std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(std::find(ind_col_0.cbegin(), ind_col_0.cend(), index_int), ind_col_0.cend());
  EXPECT_NE(std::find(ind_col_0.cbegin(), ind_col_0.cend(), index_int_str), ind_col_0.cend());

  c->remove_index(index_int_str);
  ind_col_0 = c->get_indices(std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(std::find(ind_col_0.cbegin(), ind_col_0.cend(), index_int_str), ind_col_0.cend());

  c->remove_index(index_str);
  auto ind_col_1 = c->get_indices(std::vector<ColumnID>{ColumnID{1}});
  EXPECT_EQ(std::find(ind_col_0.cbegin(), ind_col_0.cend(), index_str), ind_col_0.cend());
}

}  // namespace opossum
