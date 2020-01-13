#include "../base_test.hpp"

#include "utils/column_ids_after_pruning.hpp"

namespace opossum {

class ColumnIdsAfterPruningTest : public BaseTest {};

TEST_F(ColumnIdsAfterPruningTest, ColumnIDMapping) {
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

}  // namespace opossum
