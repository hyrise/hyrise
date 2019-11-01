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

    ds_int = encode_and_compress_segment(vs_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
    ds_str = encode_and_compress_segment(vs_str, DataType::String, SegmentEncodingSpec{EncodingType::Dictionary});

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

TEST_F(StorageChunkTest, FinalizingAFinalizedChunkThrows) {
  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}));
  chunk->append({2, "two"});

  chunk->finalize();

  EXPECT_THROW(chunk->finalize(), std::logic_error);
}

TEST_F(StorageChunkTest, FinalizeSetsMaxBeginCid) {
  auto mvcc_data = std::make_shared<MvccData>(3, 0);
  mvcc_data->begin_cids = {1, 2, 3};

  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}), mvcc_data);
  chunk->finalize();

  auto mvcc_data_chunk = chunk->get_scoped_mvcc_data_lock();
  EXPECT_EQ(mvcc_data_chunk->max_begin_cid, 3);
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

TEST_F(StorageChunkTest, GetIndexesByColumnIDs) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str});

  auto indexes_for_segment_0 = chunk->get_indexes(std::vector<ColumnID>{ColumnID{0}});
  // Make sure it finds both the single-column index as well as the multi-column index
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int),
            indexes_for_segment_0.cend());
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int_str),
            indexes_for_segment_0.cend());

  auto indexes_for_segment_1 = chunk->get_indexes(std::vector<ColumnID>{ColumnID{1}});
  // Make sure it only finds the single-column index
  EXPECT_NE(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_str),
            indexes_for_segment_1.cend());
  EXPECT_EQ(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_int_str),
            indexes_for_segment_1.cend());
}

TEST_F(StorageChunkTest, GetIndexesBySegmentPointers) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str});

  auto indexes_for_segment_0 = chunk->get_indexes(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  // Make sure it finds both the single-column index as well as the multi-column index
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int),
            indexes_for_segment_0.cend());
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int_str),
            indexes_for_segment_0.cend());

  auto indexes_for_segment_1 = chunk->get_indexes(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  // Make sure it only finds the single-column index
  EXPECT_NE(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_str),
            indexes_for_segment_1.cend());
  EXPECT_EQ(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_int_str),
            indexes_for_segment_1.cend());
}

TEST_F(StorageChunkTest, RemoveIndex) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const BaseSegment>>{ds_int, ds_str});

  chunk->remove_index(index_int);
  auto indexes_for_segment_0 = chunk->get_indexes(std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int),
            indexes_for_segment_0.cend());
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int_str),
            indexes_for_segment_0.cend());

  chunk->remove_index(index_int_str);
  indexes_for_segment_0 = chunk->get_indexes(std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int_str),
            indexes_for_segment_0.cend());

  chunk->remove_index(index_str);
  auto indexes_for_segment_1 = chunk->get_indexes(std::vector<ColumnID>{ColumnID{1}});
  EXPECT_EQ(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_str),
            indexes_for_segment_0.cend());
}

TEST_F(StorageChunkTest, OrderedBy) {
  EXPECT_EQ(chunk->ordered_by(), std::nullopt);
  const auto ordered_by = std::make_pair(ColumnID(0), OrderByMode::Ascending);
  chunk->set_ordered_by(ordered_by);
  EXPECT_EQ(chunk->ordered_by(), ordered_by);
}

}  // namespace opossum
