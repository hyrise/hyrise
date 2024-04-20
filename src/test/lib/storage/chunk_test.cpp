#include <memory>

#include "base_test.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "types.hpp"

namespace hyrise {

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
  EXPECT_EQ(chunk->size(), 0);
  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}));
  EXPECT_EQ(chunk->size(), 3);
}

TEST_F(StorageChunkTest, AddValuesToChunk) {
  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}));
  chunk->append({2, "two"});
  EXPECT_EQ(chunk->size(), 4);

  if (HYRISE_DEBUG) {
    EXPECT_THROW(chunk->append({}), std::exception);
    EXPECT_THROW(chunk->append({4, "val", 3}), std::exception);
    EXPECT_EQ(chunk->size(), 4);
  }
}

TEST_F(StorageChunkTest, RetrieveSegment) {
  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}));
  chunk->append({2, "two"});

  const auto abstract_segment = chunk->get_segment(ColumnID{0});
  EXPECT_EQ(abstract_segment->size(), 4);
}

TEST_F(StorageChunkTest, SetImmutableTwiceThrows) {
  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}));
  chunk->append({2, "two"});

  chunk->set_immutable();

  EXPECT_THROW(chunk->set_immutable(), std::logic_error);
}

TEST_F(StorageChunkTest, SetImmutableSetsMaxBeginCid) {
  const auto mvcc_data = std::make_shared<MvccData>(3, CommitID{0});
  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{2});
  mvcc_data->set_begin_cid(ChunkOffset{2}, CommitID{3});

  chunk = std::make_shared<Chunk>(Segments({vs_int, vs_str}), mvcc_data);
  chunk->set_immutable();

  const auto mvcc_data_chunk = chunk->mvcc_data();
  EXPECT_EQ(mvcc_data_chunk->max_begin_cid.load(), 3);
}

TEST_F(StorageChunkTest, AddIndexByColumnID) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  const auto index_int = chunk->create_index<GroupKeyIndex>(std::vector<ColumnID>{ColumnID{0}});
  const auto index_str = chunk->create_index<GroupKeyIndex>(std::vector<ColumnID>{ColumnID{0}});
  const auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<ColumnID>{ColumnID{0}, ColumnID{1}});
  EXPECT_TRUE(index_int);
  EXPECT_TRUE(index_str);
  EXPECT_TRUE(index_int_str);
}

TEST_F(StorageChunkTest, AddIndexBySegmentPointer) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  const auto index_int =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  const auto index_str =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  const auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});
  EXPECT_TRUE(index_int);
  EXPECT_TRUE(index_str);
  EXPECT_TRUE(index_int_str);
}

TEST_F(StorageChunkTest, GetIndexByColumnID) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  const auto index_int =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  const auto index_str =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  const auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});

  EXPECT_EQ(chunk->get_index(ChunkIndexType::GroupKey, std::vector<ColumnID>{ColumnID{0}}), index_int);
  EXPECT_EQ(chunk->get_index(ChunkIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{0}}), index_int_str);
  EXPECT_EQ(chunk->get_index(ChunkIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{0}, ColumnID{1}}),
            index_int_str);
  EXPECT_EQ(chunk->get_index(ChunkIndexType::GroupKey, std::vector<ColumnID>{ColumnID{1}}), index_str);
  EXPECT_EQ(chunk->get_index(ChunkIndexType::CompositeGroupKey, std::vector<ColumnID>{ColumnID{1}}), nullptr);
}

TEST_F(StorageChunkTest, GetIndexBySegmentPointer) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  const auto index_int =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  const auto index_str =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  const auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});

  EXPECT_EQ(chunk->get_index(ChunkIndexType::GroupKey, std::vector<std::shared_ptr<const AbstractSegment>>{ds_int}),
            index_int);
  EXPECT_EQ(
      chunk->get_index(ChunkIndexType::CompositeGroupKey, std::vector<std::shared_ptr<const AbstractSegment>>{ds_int}),
      index_int_str);
  EXPECT_EQ(chunk->get_index(ChunkIndexType::CompositeGroupKey,
                             std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str}),
            index_int_str);
  EXPECT_EQ(chunk->get_index(ChunkIndexType::GroupKey, std::vector<std::shared_ptr<const AbstractSegment>>{ds_str}),
            index_str);
  EXPECT_EQ(
      chunk->get_index(ChunkIndexType::CompositeGroupKey, std::vector<std::shared_ptr<const AbstractSegment>>{ds_str}),
      nullptr);
}

TEST_F(StorageChunkTest, GetIndexesByColumnIDs) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  const auto index_int =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  const auto index_str =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  const auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});

  const auto indexes_for_segment_0 = chunk->get_indexes(std::vector<ColumnID>{ColumnID{0}});
  // Make sure it finds both the single-column index as well as the multi-column index
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int),
            indexes_for_segment_0.cend());
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int_str),
            indexes_for_segment_0.cend());

  const auto indexes_for_segment_1 = chunk->get_indexes(std::vector<ColumnID>{ColumnID{1}});
  // Make sure it only finds the single-column index
  EXPECT_NE(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_str),
            indexes_for_segment_1.cend());
  EXPECT_EQ(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_int_str),
            indexes_for_segment_1.cend());
}

TEST_F(StorageChunkTest, GetIndexesBySegmentPointers) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  const auto index_int =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  const auto index_str =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  const auto index_int_str =
      chunk->create_index<CompositeGroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int, ds_str});

  const auto indexes_for_segment_0 = chunk->get_indexes(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  // Make sure it finds both the single-column index as well as the multi-column index
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int),
            indexes_for_segment_0.cend());
  EXPECT_NE(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_int_str),
            indexes_for_segment_0.cend());

  const auto indexes_for_segment_1 = chunk->get_indexes(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  // Make sure it only finds the single-column index
  EXPECT_NE(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_str),
            indexes_for_segment_1.cend());
  EXPECT_EQ(std::find(indexes_for_segment_1.cbegin(), indexes_for_segment_1.cend(), index_int_str),
            indexes_for_segment_1.cend());
}

TEST_F(StorageChunkTest, RemoveIndex) {
  chunk = std::make_shared<Chunk>(Segments({ds_int, ds_str}));
  const auto index_int =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_int});
  const auto index_str =
      chunk->create_index<GroupKeyIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{ds_str});
  const auto index_int_str =
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
  const auto indexes_for_segment_1 = chunk->get_indexes(std::vector<ColumnID>{ColumnID{1}});
  EXPECT_EQ(std::find(indexes_for_segment_0.cbegin(), indexes_for_segment_0.cend(), index_str),
            indexes_for_segment_0.cend());
}

TEST_F(StorageChunkTest, SetSortedInformationSingle) {
  EXPECT_TRUE(chunk->individually_sorted_by().empty());
  const auto sorted_by = SortColumnDefinition(ColumnID{0}, SortMode::Ascending);
  chunk->set_immutable();
  chunk->set_individually_sorted_by(sorted_by);
  EXPECT_EQ(chunk->individually_sorted_by().size(), 1);
  EXPECT_EQ(chunk->individually_sorted_by().front(), sorted_by);
}

TEST_F(StorageChunkTest, SetSortedInformationVector) {
  EXPECT_TRUE(chunk->individually_sorted_by().empty());
  const auto sorted_by_vector = std::vector{SortColumnDefinition(ColumnID{0}, SortMode::Ascending),
                                            SortColumnDefinition(ColumnID{1}, SortMode::Descending)};
  chunk->set_immutable();
  chunk->set_individually_sorted_by(sorted_by_vector);
  EXPECT_EQ(chunk->individually_sorted_by(), sorted_by_vector);

  // Resetting the sorting information is not allowed
  EXPECT_THROW(chunk->set_individually_sorted_by(sorted_by_vector), std::logic_error);
}

TEST_F(StorageChunkTest, SetSortedInformationAscendingWithNulls) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  const auto value_segment = std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{17, 0, 1, 1},
                                                                     pmr_vector<bool>{true, true, false, false});
  const auto chunk_with_nulls = std::make_shared<Chunk>(Segments{value_segment});
  chunk_with_nulls->set_immutable();
  EXPECT_TRUE(chunk_with_nulls->individually_sorted_by().empty());

  EXPECT_NO_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending)));
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Descending)),
               std::logic_error);
}

TEST_F(StorageChunkTest, SetSortedInformationDescendingWithNulls) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  const auto value_segment = std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{0, 2, 1, 1},
                                                                     pmr_vector<bool>{true, false, false, false});
  const auto chunk_with_nulls = std::make_shared<Chunk>(Segments{value_segment});
  chunk_with_nulls->set_immutable();
  EXPECT_TRUE(chunk_with_nulls->individually_sorted_by().empty());

  // Currently, NULL values always come first when sorted.
  EXPECT_NO_THROW(
      chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Descending)));
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending)),
               std::logic_error);
}

TEST_F(StorageChunkTest, SetSortedInformationUnsortedNULLs) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  const auto value_segment =
      std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{1, 1, 1}, pmr_vector<bool>{false, true, false});
  const auto chunk_with_nulls = std::make_shared<Chunk>(Segments{value_segment});
  chunk_with_nulls->set_immutable();
  EXPECT_TRUE(chunk_with_nulls->individually_sorted_by().empty());

  // Sorted values, but NULLs always come first in Hyrise when vector is sorted.
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending)),
               std::logic_error);
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Descending)),
               std::logic_error);
}

TEST_F(StorageChunkTest, SetSortedInformationNULLsLast) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  const auto value_segment =
      std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{1, 1, 1}, pmr_vector<bool>{false, false, true});
  const auto chunk_with_nulls = std::make_shared<Chunk>(Segments{value_segment});
  chunk_with_nulls->set_immutable();
  EXPECT_TRUE(chunk_with_nulls->individually_sorted_by().empty());

  // Sorted values, but NULLs always come first in Hyrise when vector is sorted.
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending)),
               std::logic_error);
  EXPECT_THROW(chunk_with_nulls->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Descending)),
               std::logic_error);
}

TEST_F(StorageChunkTest, MemoryUsageEstimation) {
  const auto segments = Segments{vs_int, vs_str};
  auto segment_sizes = vs_int->memory_usage(MemoryUsageCalculationMode::Sampled);
  segment_sizes += vs_str->memory_usage(MemoryUsageCalculationMode::Sampled);

  // Chunk holds at least two segments.
  chunk = std::make_shared<Chunk>(segments);
  EXPECT_GT(chunk->memory_usage(MemoryUsageCalculationMode::Sampled), segment_sizes);

  const auto mvcc_data = std::make_shared<MvccData>(ChunkOffset{3}, CommitID{0});
  const auto mvcc_size = mvcc_data->memory_usage();

  // Chunk holds at least two segments and MVCC data.
  chunk = std::make_shared<Chunk>(segments, mvcc_data);
  EXPECT_GT(chunk->memory_usage(MemoryUsageCalculationMode::Sampled), segment_sizes + mvcc_size);
}

// A concurrency stress test can be found at `stress_test.cpp` (ConcurrentInsertsSetChunksImmutable).
TEST_F(StorageChunkTest, TrySetImmutable) {
  if constexpr (HYRISE_DEBUG) {
    // try_set_immutable() should only be called when the chunk is part of a stored table, i.e., it must have MVCC data.
    EXPECT_THROW(chunk->try_set_immutable(), std::logic_error);
  }

  const auto mvcc_data = std::make_shared<MvccData>(ChunkOffset{3}, CommitID{0});
  chunk = std::make_shared<Chunk>(Segments{vs_int, vs_str}, mvcc_data);

  EXPECT_TRUE(chunk->is_mutable());

  // Nothing happens if chunk is not marked as full.
  chunk->try_set_immutable();
  EXPECT_TRUE(chunk->is_mutable());

  // Marking as immutable works if chunk is marked as full.
  chunk->mark_as_full();
  chunk->try_set_immutable();
  EXPECT_FALSE(chunk->is_mutable());

  // Multiple calls are okay.
  chunk->try_set_immutable();
  EXPECT_FALSE(chunk->is_mutable());

  // However, chunk should not be marked as full multiple times.
  EXPECT_THROW(chunk->mark_as_full(), std::logic_error);
}

}  // namespace hyrise
