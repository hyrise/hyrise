#include "base_test.hpp"

#include "utils/column_pruning_utils.hpp"

namespace hyrise {

class PruningUtilsTest : public BaseTest {};

TEST_F(PruningUtilsTest, ColumnIDMapping) {
  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(column_ids_after_pruning(4, {ColumnID{4}}), std::logic_error);
  }

  auto actual_column_mapping = column_ids_after_pruning(6, {ColumnID{0}, ColumnID{1}, ColumnID{2}});
  auto expected_column_mapping = std::vector<std::optional<ColumnID>>{std::nullopt, std::nullopt, std::nullopt,
                                                                      ColumnID{0},  ColumnID{1},  ColumnID{2}};
  EXPECT_EQ(actual_column_mapping, expected_column_mapping);

  actual_column_mapping = column_ids_after_pruning(6, {ColumnID{2}, ColumnID{3}});
  expected_column_mapping = std::vector<std::optional<ColumnID>>{ColumnID{0},  ColumnID{1}, std::nullopt,
                                                                 std::nullopt, ColumnID{2}, ColumnID{3}};
  EXPECT_EQ(actual_column_mapping, expected_column_mapping);

  actual_column_mapping = column_ids_after_pruning(6, {ColumnID{5}});
  expected_column_mapping = std::vector<std::optional<ColumnID>>{ColumnID{0}, ColumnID{1}, ColumnID{2},
                                                                 ColumnID{3}, ColumnID{4}, std::nullopt};
  EXPECT_EQ(actual_column_mapping, expected_column_mapping);

  actual_column_mapping = column_ids_after_pruning(6, {ColumnID{0}, ColumnID{2}, ColumnID{4}});
  expected_column_mapping = std::vector<std::optional<ColumnID>>{std::nullopt, ColumnID{0},  std::nullopt,
                                                                 ColumnID{1},  std::nullopt, ColumnID{2}};
  EXPECT_EQ(actual_column_mapping, expected_column_mapping);
}

TEST_F(PruningUtilsTest, ChunkIDMapping) {
  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(chunk_ids_after_pruning(4, {ChunkID{4}}), std::logic_error);
  }

  auto actual_chunk_mapping = chunk_ids_after_pruning(6, {ChunkID{0}, ChunkID{1}, ChunkID{2}});
  auto expected_chunk_mapping = std::unordered_map<ChunkID, ChunkID>{
      {ChunkID{3}, ChunkID{0}}, {ChunkID{4}, ChunkID{1}}, {ChunkID{5}, ChunkID{2}}};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);

  actual_chunk_mapping = chunk_ids_after_pruning(6, {ChunkID{2}, ChunkID{3}});
  expected_chunk_mapping = std::unordered_map<ChunkID, ChunkID>{
      {ChunkID{0}, ChunkID{0}}, {ChunkID{1}, ChunkID{1}}, {ChunkID{4}, ChunkID{2}}, {ChunkID{5}, ChunkID{3}}};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);

  actual_chunk_mapping = chunk_ids_after_pruning(5, {ChunkID{0}, ChunkID{4}});  // first and last
  expected_chunk_mapping = std::unordered_map<ChunkID, ChunkID>{
      {ChunkID{1}, ChunkID{0}}, {ChunkID{2}, ChunkID{1}}, {ChunkID{3}, ChunkID{2}}};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);

  actual_chunk_mapping = chunk_ids_after_pruning(6, {ChunkID{5}});
  expected_chunk_mapping = std::unordered_map<ChunkID, ChunkID>{{ChunkID{0}, ChunkID{0}},
                                                                {ChunkID{1}, ChunkID{1}},
                                                                {ChunkID{2}, ChunkID{2}},
                                                                {ChunkID{3}, ChunkID{3}},
                                                                {ChunkID{4}, ChunkID{4}}};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);

  actual_chunk_mapping = chunk_ids_after_pruning(6, {ChunkID{0}, ChunkID{2}, ChunkID{4}});
  expected_chunk_mapping = std::unordered_map<ChunkID, ChunkID>{
      {ChunkID{1}, ChunkID{0}}, {ChunkID{3}, ChunkID{1}}, {ChunkID{5}, ChunkID{2}}};
  EXPECT_EQ(actual_chunk_mapping, expected_chunk_mapping);
}

TEST_F(PruningUtilsTest, ColumnIDBeforePruning) {
  const auto pruned_column_ids = {ColumnID{0}, ColumnID{1}, ColumnID{2}};
  const auto column_id_after_pruning = ColumnID{0};
  const auto expected_column_id_before_pruning = ColumnID{3};

  auto actual_column_id_before_pruning = column_id_before_pruning(column_id_after_pruning, pruned_column_ids);
  EXPECT_EQ(actual_column_id_before_pruning, expected_column_id_before_pruning);
}

}  // namespace hyrise
