#include "base_test.hpp"

#include "utils/pruning_utils.hpp"

namespace hyrise {

class PruningUtilsTest : public BaseTest {};

TEST_F(PruningUtilsTest, ColumnIDMapping) {
  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(pruned_column_id_mapping(4, {ColumnID{4}}), std::logic_error);
  }

  auto actual_column_mapping = pruned_column_id_mapping(6, {ColumnID{0}, ColumnID{1}, ColumnID{2}});
  auto expected_column_mapping = std::vector<ColumnID>{INVALID_COLUMN_ID, INVALID_COLUMN_ID, INVALID_COLUMN_ID,
                                                       ColumnID{0},       ColumnID{1},       ColumnID{2}};
  EXPECT_EQ(actual_column_mapping, expected_column_mapping);

  actual_column_mapping = pruned_column_id_mapping(6, {ColumnID{2}, ColumnID{3}});
  expected_column_mapping =
      std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, INVALID_COLUMN_ID, INVALID_COLUMN_ID, ColumnID{2}, ColumnID{3}};
  EXPECT_EQ(actual_column_mapping, expected_column_mapping);

  actual_column_mapping = pruned_column_id_mapping(6, {ColumnID{5}});
  expected_column_mapping =
      std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{3}, ColumnID{4}, INVALID_COLUMN_ID};
  EXPECT_EQ(actual_column_mapping, expected_column_mapping);

  actual_column_mapping = pruned_column_id_mapping(6, {ColumnID{0}, ColumnID{2}, ColumnID{4}});
  expected_column_mapping = std::vector<ColumnID>{INVALID_COLUMN_ID, ColumnID{0},       INVALID_COLUMN_ID,
                                                  ColumnID{1},       INVALID_COLUMN_ID, ColumnID{2}};
  EXPECT_EQ(actual_column_mapping, expected_column_mapping);
}

TEST_F(PruningUtilsTest, ChunkIDMapping) {
  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(pruned_chunk_id_mapping(4, {ChunkID{4}}), std::logic_error);
  }

  auto actual_chunk_mapping = pruned_chunk_id_mapping(6, {ChunkID{0}, ChunkID{1}, ChunkID{2}});
  auto expected_chunk_mapping =
      std::vector<ChunkID>{INVALID_CHUNK_ID, INVALID_CHUNK_ID, INVALID_CHUNK_ID, ChunkID{0}, ChunkID{1}, ChunkID{2}};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);

  actual_chunk_mapping = pruned_chunk_id_mapping(6, {ChunkID{2}, ChunkID{3}});
  expected_chunk_mapping =
      std::vector<ChunkID>{ChunkID{0}, ChunkID{1}, INVALID_CHUNK_ID, INVALID_CHUNK_ID, ChunkID{2}, ChunkID{3}};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);

  actual_chunk_mapping = pruned_chunk_id_mapping(5, {ChunkID{0}, ChunkID{4}});  // first and last
  expected_chunk_mapping = std::vector<ChunkID>{INVALID_CHUNK_ID, ChunkID{0}, ChunkID{1}, ChunkID{2}, INVALID_CHUNK_ID};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);

  actual_chunk_mapping = pruned_chunk_id_mapping(6, {ChunkID{5}});
  expected_chunk_mapping =
      std::vector<ChunkID>{ChunkID{0}, ChunkID{1}, ChunkID{2}, ChunkID{3}, ChunkID{4}, INVALID_CHUNK_ID};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);

  actual_chunk_mapping = pruned_chunk_id_mapping(6, {ChunkID{0}, ChunkID{2}, ChunkID{4}});
  expected_chunk_mapping =
      std::vector<ChunkID>{INVALID_CHUNK_ID, ChunkID{0}, INVALID_CHUNK_ID, ChunkID{1}, INVALID_CHUNK_ID, ChunkID{2}};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);
}

TEST_F(PruningUtilsTest, ColumnIDBeforePruning) {
  const auto pruned_column_ids = {ColumnID{0}, ColumnID{1}, ColumnID{2}};
  const auto column_id_after_pruning = ColumnID{0};
  const auto expected_column_id_before_pruning = ColumnID{3};

  auto actual_column_id_before_pruning = column_id_before_pruning(column_id_after_pruning, pruned_column_ids);
  EXPECT_EQ(actual_column_id_before_pruning, expected_column_id_before_pruning);
}

TEST_F(PruningUtilsTest, TooManyChunksPruned) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  EXPECT_THROW(pruned_chunk_id_mapping(2, {ChunkID{0}, ChunkID{1}, ChunkID{2}}), std::logic_error);
}

}  // namespace hyrise
