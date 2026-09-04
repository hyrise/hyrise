#include <cstdint>
#include <memory>
#include <vector>

#include "base_test.hpp"
#include "operators/aggregate_dyod_utils/cardinality_estimation.hpp"
#include "operators/aggregate_dyod_utils/concurrent_ticket_map.hpp"
#include "operators/aggregate_dyod_utils/ticketing.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

namespace hyrise {

class CardinalityEstimationTest : public BaseTest {};

namespace {

void add_range(HyperLogLog<>& sketch, const uint64_t begin, const uint64_t end) {
  for (auto value = begin; value < end; ++value) {
    sketch.add(fmix64(compute_hash(&value, sizeof(value))));
  }
}

// Table with `distinct_group_count` distinct (a, b) groups, each repeated twice, split into multiple chunks.
std::shared_ptr<Table> create_two_column_table(const uint64_t distinct_group_count) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Long, false);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{1'000});

  for (auto repetition = 0; repetition < 2; ++repetition) {
    for (auto group = uint64_t{0}; group < distinct_group_count; ++group) {
      table->append({static_cast<int32_t>(group % 1000), static_cast<int64_t>(group)});
    }
  }

  return table;
}

constexpr auto DISTINCT_GROUP_COUNT = uint64_t{50'000};

}  // namespace

TEST_F(CardinalityEstimationTest, EstimateIsAccurate) {
  for (const auto cardinality : {uint64_t{10'000}, uint64_t{100'000}, uint64_t{1'000'000}}) {
    auto sketch = HyperLogLog<>{};
    add_range(sketch, 0, cardinality);

    const auto estimate = sketch.estimate();
    EXPECT_GT(estimate, static_cast<size_t>(0.9 * static_cast<double>(cardinality)));
    EXPECT_LT(estimate, static_cast<size_t>(1.1 * static_cast<double>(cardinality)));
  }
}

TEST_F(CardinalityEstimationTest, UpperBoundCover) {
  auto sketch = HyperLogLog<>{};
  add_range(sketch, 0, 100'000);

  EXPECT_GT(sketch.estimate_upper_bound(), sketch.estimate());
  EXPECT_GE(sketch.estimate_upper_bound(), 100'000);
}

TEST_F(CardinalityEstimationTest, MergeIsMaximum) {
  // Merging two disjoint halves must yield exactly the sketch of all values, as both take the per-register maximum.
  auto first_half = HyperLogLog<>{};
  add_range(first_half, 0, 50'000);
  auto second_half = HyperLogLog<>{};
  add_range(second_half, 50'000, 100'000);

  auto everything = HyperLogLog<>{};
  add_range(everything, 0, 100'000);

  first_half.merge(second_half);
  EXPECT_EQ(first_half.estimate(), everything.estimate());
}

TEST_F(CardinalityEstimationTest, EmptyTableEstimatesSingleGroup) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Long, false);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{1'000});

  const auto groupby_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}};
  const auto row_format = create_row_format(table->column_definitions(), groupby_column_ids);

  EXPECT_EQ(estimate_group_count_multi_column(row_format, groupby_column_ids, table, 1'000), 1);
  EXPECT_EQ(estimate_group_count_single_column<int32_t>(ColumnID{0}, table), 1);
}

TEST_F(CardinalityEstimationTest, MultiColumnEstimateIsCloseToGroupCount) {
  const auto table = create_two_column_table(DISTINCT_GROUP_COUNT);
  const auto groupby_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}};
  const auto row_format = create_row_format(table->column_definitions(), groupby_column_ids);

  const auto estimate = estimate_group_count_multi_column(row_format, groupby_column_ids, table, 1'000);

  EXPECT_GE(estimate, DISTINCT_GROUP_COUNT);
  EXPECT_LT(estimate, static_cast<size_t>(1.2 * static_cast<double>(DISTINCT_GROUP_COUNT)));
}

TEST_F(CardinalityEstimationTest, SingleColumnEstimateIsCloseToGroupCount) {
  const auto table = create_two_column_table(DISTINCT_GROUP_COUNT);
  const auto estimate = estimate_group_count_single_column<int64_t>(ColumnID{1}, table);

  EXPECT_GE(estimate, DISTINCT_GROUP_COUNT);
  EXPECT_LT(estimate, static_cast<size_t>(1.2 * static_cast<double>(DISTINCT_GROUP_COUNT)));
}

}  // namespace hyrise
