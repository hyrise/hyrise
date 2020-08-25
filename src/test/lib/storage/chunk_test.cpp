#include <memory>

#include "base_test.hpp"

#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "types.hpp"

namespace opossum {

class StorageChunkTest : public BaseTest {
 protected:
  void SetUp() override {
    vs_int = std::make_shared<ValueSegment<int>>();
    vs_int->append(4);
    vs_int->append(6);
    vs_int->append(3);

    vs_str = std::make_shared<ValueSegment<pmr_string>>();
    vs_str->append("Hello,");
    vs_str->append("world");
    vs_str->append("!");

    ds_int = ChunkEncoder::encode_segment(vs_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
    ds_str = ChunkEncoder::encode_segment(vs_str, DataType::String, SegmentEncodingSpec{EncodingType::Dictionary});

    Segments empty_segments;
    empty_segments.push_back(std::make_shared<ValueSegment<int32_t>>());
    empty_segments.push_back(std::make_shared<ValueSegment<pmr_string>>());

    chunk = std::make_shared<Chunk>(empty_segments);
  }

  std::shared_ptr<Chunk> chunk;
  std::shared_ptr<BaseValueSegment> vs_int = nullptr;
  std::shared_ptr<BaseValueSegment> vs_str = nullptr;
  std::shared_ptr<AbstractSegment> ds_int = nullptr;
  std::shared_ptr<AbstractSegment> ds_str = nullptr;
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

  auto abstract_segment = chunk->get_segment(ColumnID{0});
  EXPECT_EQ(abstract_segment->size(), 4u);
}

TEST_F(StorageChunkTest, FinalizingAFinalizedChunkThrows) {
  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}));
  chunk->append({2, "two"});

  chunk->finalize();

  EXPECT_THROW(chunk->finalize(), std::logic_error);
}

TEST_F(StorageChunkTest, FinalizeSetsMaxBeginCid) {
  auto mvcc_data = std::make_shared<MvccData>(3, 0);
  mvcc_data->set_begin_cid(0, 1);
  mvcc_data->set_begin_cid(1, 2);
  mvcc_data->set_begin_cid(2, 3);

  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}), mvcc_data);
  chunk->finalize();

  auto mvcc_data_chunk = chunk->mvcc_data();
  EXPECT_EQ(mvcc_data_chunk->max_begin_cid, 3);
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
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});
  EXPECT_TRUE(index_int);
  EXPECT_TRUE(index_str);
  EXPECT_TRUE(index_int_str);
}

TEST_F(StorageChunkTest, GetIndexByColumnID) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});

  EXPECT_EQ(chunk->get_index(SegmentIndexType::GroupKey, std::vector<ColumnID>{ColumnID{0}}), index_int);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{0}}), index_int_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{0}, ColumnID{1}}),
            index_int_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::GroupKey, std::vector<ColumnID>{ColumnID{1}}), index_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{1}}), nullptr);
}

TEST_F(StorageChunkTest, GetIndexBySegmentPointer) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});

  EXPECT_EQ(chunk->get_index(SegmentIndexType::GroupKey, std::vector<std::shared_ptr<const AbstractSegment>>{ds_int}),
            index_int);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey,
                             std::vector<std::shared_ptr<const AbstractSegment>>{ds_int}),
            index_int_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey,
                             std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str}),
            index_int_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::GroupKey, std::vector<std::shared_ptr<const AbstractSegment>>{ds_str}),
            index_str);
  EXPECT_EQ(chunk->get_index(SegmentIndexType::CompositeGroupKey,
                             std::vector<std::shared_ptr<const AbstractSegment>>{ds_str}),
            nullptr);
}

TEST_F(StorageChunkTest, GetIndexesByColumnIDs) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});

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
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});

  auto indexes_for_segment_0 = chunk->get_indexes(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  // Make sure it finds both the single-column index as well as the multi-column index
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int),
            indexes_for_segment_0.cend());
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int_str),
            indexes_for_segment_0.cend());

  auto indexes_for_segment_1 = chunk->get_indexes(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  // Make sure it only finds the single-column index
  EXPECT_NE(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_str),
            indexes_for_segment_1.cend());
  EXPECT_EQ(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_int_str),
            indexes_for_segment_1.cend());
}

TEST_F(StorageChunkTest, RemoveIndex) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});

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

TEST_F(StorageChunkTest, SetSortedInformationSingle) {
  EXPECT_TRUE(chunk->individually_sorted_by().empty());
  const auto sorted_by = SortColumnDefinition(ColumnID{0}, SortMode::Ascending);
  chunk->finalize();
  chunk->set_individually_sorted_by(sorted_by);
  EXPECT_EQ(chunk->individually_sorted_by().size(), 1);
  EXPECT_EQ(chunk->individually_sorted_by().front(), sorted_by);
}

TEST_F(StorageChunkTest, SetSortedInformationVector) {
  EXPECT_TRUE(chunk->individually_sorted_by().empty());
  const auto sorted_by_vector = std::vector{SortColumnDefinition(ColumnID{0}, SortMode::Ascending),
                                            SortColumnDefinition(ColumnID{1}, SortMode::Descending)};
  chunk->finalize();
  chunk->set_individually_sorted_by(sorted_by_vector);
  EXPECT_EQ(chunk->individually_sorted_by(), sorted_by_vector);

  // Resetting the sorting information is not allowed
  EXPECT_THROW(chunk->set_individually_sorted_by(sorted_by_vector), std::logic_error);
}

TEST_F(StorageChunkTest, SetSortedInformationAscendingWithNulls) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  auto value_segment = std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{17, 0, 1, 1},
                                                               pmr_vector<bool>{true, true, false, false});
  auto chunk_with_nulls = std::make_shared<Chunk>(Segments{value_segment});
  chunk_with_nulls->finalize();
  EXPECT_TRUE(chunk_with_nulls->individually_sorted_by().empty());

  EXPECT_NO_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending)));
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Descending)),
               std::logic_error);
}

TEST_F(StorageChunkTest, SetSortedInformationDescendingWithNulls) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  auto value_segment = std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{0, 2, 1, 1},
                                                               pmr_vector<bool>{true, false, false, false});
  auto chunk_with_nulls = std::make_shared<Chunk>(Segments{value_segment});
  chunk_with_nulls->finalize();
  EXPECT_TRUE(chunk_with_nulls->individually_sorted_by().empty());

  // Currently, NULL values always come first when sorted.
  EXPECT_NO_THROW(
      chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Descending)));
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending)),
               std::logic_error);
}

TEST_F(StorageChunkTest, SetSortedInformationUnsortedNULLs) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  auto value_segment =
      std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{1, 1, 1}, pmr_vector<bool>{false, true, false});
  auto chunk_with_nulls = std::make_shared<Chunk>(Segments{value_segment});
  chunk_with_nulls->finalize();
  EXPECT_TRUE(chunk_with_nulls->individually_sorted_by().empty());

  // Sorted values, but NULLs always come first in Hyrise when vector is sorted.
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending)),
               std::logic_error);
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Descending)),
               std::logic_error);
}

TEST_F(StorageChunkTest, SetSortedInformationNULLsLast) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  auto value_segment =
      std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{1, 1, 1}, pmr_vector<bool>{false, false, true});
  auto chunk_with_nulls = std::make_shared<Chunk>(Segments{value_segment});
  chunk_with_nulls->finalize();
  EXPECT_TRUE(chunk_with_nulls->individually_sorted_by().empty());

  // Sorted values, but NULLs always come first in Hyrise when vector is sorted.
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending)),
               std::logic_error);
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Descending)),
               std::logic_error);
}

}  // namespace opossum
