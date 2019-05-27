#include "storage/column_iterable.hpp"
#include "storage/table.hpp"
#include "base_test.hpp"

namespace opossum {

class ColumnIterableTest : public BaseTest {};

TEST_F(ColumnIterableTest, ForEach) {
  const auto table_column_definitions = TableColumnDefinitions{
    {"a", DataType::Int}
  };
  const auto table = std::make_shared<Table>(table_column_definitions, TableType::Data, 2);

  table->append({0});
  table->append({1});
  table->append({2});
  table->append({3});
  table->append({4});
  table->append({5});
  table->append({6});

  ColumnIterable column_iterable{table, ColumnID{0}};

  auto actual_values_a = std::vector<int32_t>{};
  column_iterable.for_each<int32_t>([&](const auto& segment_postition, const RowID& row_id) {
    actual_values_a.emplace_back(segment_postition.value());
  });
  EXPECT_EQ(actual_values_a, std::vector<int32_t>({0, 1, 2, 3, 4, 5, 6}));

  auto actual_values_b = std::vector<int32_t>{};
  column_iterable.for_each<int32_t>([&](const auto& segment_postition, const RowID& row_id) {
    actual_values_b.emplace_back(segment_postition.value());
    return segment_postition.value() == 5 ? ColumnIteration::Break : ColumnIteration::Continue;
  }, RowID{ChunkID{0}, ChunkOffset{1}});
  EXPECT_EQ(actual_values_b, std::vector<int32_t>({1, 2, 3, 4, 5}));

  auto actual_values_c = std::vector<int32_t>{};
  auto actual_row_ids_c = std::vector<RowID>{};
  column_iterable.for_each<int32_t>([&](const auto& segment_postition, const RowID& row_id) {
    actual_values_c.emplace_back(segment_postition.value());
    actual_row_ids_c.emplace_back(row_id);
  }, RowID{ChunkID{2}, ChunkOffset{1}});
  EXPECT_EQ(actual_values_c, std::vector<int32_t>({5, 6}));
  EXPECT_EQ(actual_row_ids_c, std::vector<RowID>({{ChunkID{2}, ChunkOffset{1}}, {ChunkID{3}, ChunkOffset{0}}}));
}

}  // namespace opossum
