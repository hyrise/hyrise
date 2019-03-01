#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "resolve_type.hpp"
#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "types.hpp"

namespace opossum {

class StorageChunkTest : public BaseTest {
 protected:
  void SetUp() override {
    vs_int = make_shared_by_data_type<BaseValueSegment, ValueSegment>(DataType::Int);
    vs_int->append(4);  // check for vs_ as well
    vs_int->append(6);
    vs_int->append(3);

    vs_str = make_shared_by_data_type<BaseValueSegment, ValueSegment>(DataType::String);
    vs_str->append("Hello,");
    vs_str->append("world");
    vs_str->append("!");

    ds_int = encode_segment(EncodingType::Dictionary, DataType::Int, vs_int);
    ds_str = encode_segment(EncodingType::Dictionary, DataType::String, vs_str);

    Segments empty_segments;
    empty_segments.push_back(std::make_shared<ValueSegment<int32_t>>());
    empty_segments.push_back(std::make_shared<ValueSegment<pmr_string>>());

    chunk = std::make_shared<Chunk>(empty_segments);
  }

  std::shared_ptr<Chunk> chunk;
  std::shared_ptr<BaseValueSegment> vs_int = nullptr;
  std::shared_ptr<BaseValueSegment> vs_str = nullptr;
  std::shared_ptr<BaseSegment> ds_int = nullptr;
  std::shared_ptr<BaseSegment> ds_str = nullptr;
};

TEST_F(StorageChunkTest, AddSegmentToChunk) {
  EXPECT_EQ(chunk->size(), 0u);
  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}));
  EXPECT_EQ(chunk->size(), 3u);
}

TEST_F(StorageChunkTest, AddValuesToChunk) {
  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}));
  chunk->append({2, "two"});
  EXPECT_EQ(chunk->size(), 4u);

  if (HYRISE_DEBUG) {
    EXPECT_THROW(chunk->append({}), std::exception);
    EXPECT_THROW(chunk->append({4, "val", 3}), std::exception);
    EXPECT_EQ(chunk->size(), 4u);
  }
}

TEST_F(StorageChunkTest, RetrieveSegment) {
  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}));
  chunk->append({2, "two"});

  auto base_segment = chunk->get_segment(ColumnID{0});
  EXPECT_EQ(base_segment->size(), 4u);
}

TEST_F(StorageChunkTest, UnknownColumnType) {
  // Exception will only be thrown in debug builds
  if (!HYRISE_DEBUG) GTEST_SKIP();
  auto wrapper = []() { make_shared_by_data_type<BaseSegment, ValueSegment>(DataType::Null); };
  EXPECT_THROW(wrapper(), std::logic_error);
}

TEST_F(StorageChunkTest, AddIndexByColumnID) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<ColumnID>{ColumnID{0}});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<ColumnID>{ColumnID{0}});
  auto index_int_str = chunk->create_index<CompositeGroupKeyIndex>(std::vector<ColumnID>{ColumnID{0}, ColumnID{1}});
  EXPECT_TRUE(index_int);
  EXPECT_TRUE(index_str);
  EXPECT_TRUE(index_int_str);
}

TEST_F(StorageChunkTest, AddIndexBySegmentPointer) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str});
  EXPECT_TRUE(index_int);
  EXPECT_TRUE(index_str);
  EXPECT_TRUE(index_int_str);
}

TEST_F(StorageChunkTest, GetIndexByColumnID) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str});

  EXPECT_EQ(chunk->get_index(SegmentIndexType::GroupKey, std::vector<ColumnID>{ColumnID{0}}), index_int);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{0}}), index_int_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{0}, ColumnID{1}}),
            index_int_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::GroupKey, std::vector<ColumnID>{ColumnID{1}}), index_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{1}}), nullptr);
}

TEST_F(StorageChunkTest, GetIndexBySegmentPointer) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str});

  EXPECT_EQ(chunk->get_index(SegmentIndexType::GroupKey, std::vector<std::shared_ptr<const BaseSegment>>{ds_int}),
            index_int);
  EXPECT_EQ(
      chunk->get_index(SegmentIndexType::CompositeGroupKey, std::vector<std::shared_ptr<const BaseSegment>>{ds_int}),
      index_int_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey,
                             std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str}),
            index_int_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::GroupKey, std::vector<std::shared_ptr<const BaseSegment>>{ds_str}),
            index_str);
  EXPECT_EQ(
      chunk->get_index(SegmentIndexType::CompositeGroupKey, std::vector<std::shared_ptr<const BaseSegment>>{ds_str}),
      nullptr);
}

TEST_F(StorageChunkTest, GetIndicesByColumnIDs) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str});

  auto indices_for_segment_0 = chunk->get_indices(std::vector<ColumnID>{ColumnID{0}});
  // Make sure it finds both the single-column index as well as the multi-column index
  EXPECT_NE(std::find(indices_for_segment_0.cbegin(), indices_for_segment_0.cend(), index_int),
            indices_for_segment_0.cend());
  EXPECT_NE(std::find(indices_for_segment_0.cbegin(), indices_for_segment_0.cend(), index_int_str),
            indices_for_segment_0.cend());

  auto indices_for_segment_1 = chunk->get_indices(std::vector<ColumnID>{ColumnID{1}});
  // Make sure it only finds the single-column index
  EXPECT_NE(std::find(indices_for_segment_1.cbegin(), indices_for_segment_1.cend(), index_str),
            indices_for_segment_1.cend());
  EXPECT_EQ(std::find(indices_for_segment_1.cbegin(), indices_for_segment_1.cend(), index_int_str),
            indices_for_segment_1.cend());
}

TEST_F(StorageChunkTest, GetIndicesBySegmentPointers) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str});

  auto indices_for_segment_0 = chunk->get_indices(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  // Make sure it finds both the single-column index as well as the multi-column index
  EXPECT_NE(std::find(indices_for_segment_0.cbegin(), indices_for_segment_0.cend(), index_int),
            indices_for_segment_0.cend());
  EXPECT_NE(std::find(indices_for_segment_0.cbegin(), indices_for_segment_0.cend(), index_int_str),
            indices_for_segment_0.cend());

  auto indices_for_segment_1 = chunk->get_indices(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  // Make sure it only finds the single-column index
  EXPECT_NE(std::find(indices_for_segment_1.cbegin(), indices_for_segment_1.cend(), index_str),
            indices_for_segment_1.cend());
  EXPECT_EQ(std::find(indices_for_segment_1.cbegin(), indices_for_segment_1.cend(), index_int_str),
            indices_for_segment_1.cend());
}

TEST_F(StorageChunkTest, RemoveIndex) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str});

  chunk->remove_index(index_int);
  auto indices_for_segment_0 = chunk->get_indices(std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(std::find(indices_for_segment_0.cbegin(), indices_for_segment_0.cend(), index_int),
            indices_for_segment_0.cend());
  EXPECT_NE(std::find(indices_for_segment_0.cbegin(), indices_for_segment_0.cend(), index_int_str),
            indices_for_segment_0.cend());

  chunk->remove_index(index_int_str);
  indices_for_segment_0 = chunk->get_indices(std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(std::find(indices_for_segment_0.cbegin(), indices_for_segment_0.cend(), index_int_str),
            indices_for_segment_0.cend());

  chunk->remove_index(index_str);
  auto indices_for_segment_1 = chunk->get_indices(std::vector<ColumnID>{ColumnID{1}});
  EXPECT_EQ(std::find(indices_for_segment_0.cbegin(), indices_for_segment_0.cend(), index_str),
            indices_for_segment_0.cend());
}

}  // namespace opossum
