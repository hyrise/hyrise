#include "storage/column_iterable.hpp"
#include "storage/table.hpp"
#include "base_test.hpp"

namespace opossum {

class ColumnIterableTest : public BaseTest {
 public:
  void SetUp() override {
    const auto table_column_definitions = TableColumnDefinitions{
    {"a", DataType::Int}
    };
    table = std::make_shared<Table>(table_column_definitions, TableType::Data, 3);

    table->append({0});
    table->append({1});
    table->append({2});

    table->append({3});
    table->append({4});
    table->append({5});

    table->append({6});
  }

  std::shared_ptr<Table> table;
};

TEST_F(ColumnIterableTest, ForEachReturnsVoid) {
  ColumnIterable column_iterable{table, ColumnID{0}};

  auto actual_values_a = std::vector<int32_t>{};
  auto end_row_id = column_iterable.for_each<int32_t>([&](const auto& segment_postition, const RowID& row_id) {
    actual_values_a.emplace_back(segment_postition.value());
  });
  EXPECT_EQ(end_row_id, RowID(ChunkID{3}, ChunkOffset{0}));
  EXPECT_EQ(actual_values_a, std::vector<int32_t>({0, 1, 2, 3, 4, 5, 6}));
}

TEST_F(ColumnIterableTest, ForEachFromRowID) {
  ColumnIterable column_iterable{table, ColumnID{0}};

  auto actual_values_b = std::vector<int32_t>{};
  auto end_row_id_b = column_iterable.for_each<int32_t>([&](const auto& segment_postition, const RowID& row_id) {
    actual_values_b.emplace_back(segment_postition.value());
    return segment_postition.value() == 4 ? ColumnIteration::Break : ColumnIteration::Continue;
  }, RowID{ChunkID{0}, ChunkOffset{1}});
  EXPECT_EQ(end_row_id_b, RowID(ChunkID{1}, ChunkOffset{2}));
  EXPECT_EQ(actual_values_b, std::vector<int32_t>({1, 2, 3, 4}));

  auto actual_values_c = std::vector<int32_t>{};
  auto actual_row_ids_c = std::vector<RowID>{};
  auto end_row_id_c = column_iterable.for_each<int32_t>([&](const auto& segment_postition, const RowID& row_id) {
    actual_values_c.emplace_back(segment_postition.value());
    actual_row_ids_c.emplace_back(row_id);
  }, RowID{ChunkID{1}, ChunkOffset{2}});
  EXPECT_EQ(end_row_id_c, RowID(ChunkID{3}, ChunkOffset{0}));
  EXPECT_EQ(actual_values_c, std::vector<int32_t>({5, 6}));
  EXPECT_EQ(actual_row_ids_c, std::vector<RowID>({{ChunkID{1}, ChunkOffset{2}}, {ChunkID{2}, ChunkOffset{0}}}));
}

TEST_F(ColumnIterableTest, ForEachToFullLastChunk) {
  // Fill last Chunk
  table->append({7});
  table->append({8});

  ColumnIterable column_iterable{table, ColumnID{0}};

  auto actual_values_d = std::vector<int32_t>{};
  auto end_row_id_b = column_iterable.for_each<int32_t>([&](const auto& segment_postition, const RowID& row_id) {
    actual_values_d.emplace_back(segment_postition.value());
  }, RowID{ChunkID{0}, ChunkOffset{0}});
  EXPECT_EQ(end_row_id_b, RowID(ChunkID{3}, ChunkOffset{0}));
  EXPECT_EQ(actual_values_d, std::vector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8}));
}

}  // namespace opossum
