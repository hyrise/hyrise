#include "gtest/gtest.h"

#include "utils/pruning_column_id_mapping.hpp"

namespace opossum {

TEST(ColumnPruning, ColumnIDMapping) {
  auto actual_column_mapping_1 = column_ids_after_pruning(6, {ColumnID{0}, ColumnID{1}, ColumnID{2}});
  auto actual_column_mapping_2 = column_ids_after_pruning(6, {ColumnID{2}, ColumnID{3}});
  auto actual_column_mapping_3 = column_ids_after_pruning(6, {ColumnID{5}});
  auto actual_column_mapping_4 = column_ids_after_pruning(6, {ColumnID{0}, ColumnID{2}, ColumnID{4}});

  auto expected_column_mapping_1 = std::vector<std::optional<ColumnID>>{std::nullopt, std::nullopt, std::nullopt,
                                                                        ColumnID{0},  ColumnID{1},  ColumnID{2}};
  auto expected_column_mapping_2 = std::vector<std::optional<ColumnID>>{ColumnID{0},  ColumnID{1}, std::nullopt,
                                                                        std::nullopt, ColumnID{2}, ColumnID{3}};
  auto expected_column_mapping_3 = std::vector<std::optional<ColumnID>>{ColumnID{0}, ColumnID{1}, ColumnID{2},
                                                                        ColumnID{3}, ColumnID{4}, std::nullopt};
  auto expected_column_mapping_4 = std::vector<std::optional<ColumnID>>{std::nullopt, ColumnID{0},  std::nullopt,
                                                                        ColumnID{1},  std::nullopt, ColumnID{2}};

  EXPECT_EQ(actual_column_mapping_1, expected_column_mapping_1);
  EXPECT_EQ(actual_column_mapping_2, expected_column_mapping_2);
  EXPECT_EQ(actual_column_mapping_3, expected_column_mapping_3);
  EXPECT_EQ(actual_column_mapping_4, expected_column_mapping_4);
}

}  // namespace opossum
