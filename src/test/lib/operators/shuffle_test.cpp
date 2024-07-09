#include <memory>
#include <random>
#include <unordered_set>
#include <vector>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/progressive/shuffle.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"


// https://stackoverflow.com/a/72073933/1147726
template<>
struct std::hash<std::array<int32_t, 3>>
{
  size_t operator()(const std::array<int32_t, 3>& vector) const noexcept {
    auto seed = vector.size();
    for (auto elemment : vector) {
      elemment = ((elemment >> 16) ^ elemment) * 0x45d9f3b;
      elemment = ((elemment >> 16) ^ elemment) * 0x45d9f3b;
      elemment = (elemment >> 16) ^ elemment;
      seed ^= elemment + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class OperatorsShuffleTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(OperatorsShuffleTest, Name) {
  const auto table_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
  const auto shuffle = std::make_shared<Shuffle>(table_wrapper, std::vector<ColumnID>{ColumnID{0}}, std::vector<uint8_t>{uint8_t{8}});
  EXPECT_EQ(shuffle->name(), "Shuffle");
}

TEST_F(OperatorsShuffleTest, SingleColumn) {
  constexpr auto PARTITION_COUNT = uint8_t{8};
  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}};
  const auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{4}, UseMvcc::Yes);

  for (auto value = int32_t{0}; value < 64; ++value) {
    table->append({value});
  }

  Hyrise::get().storage_manager.add_table("table_a", table);

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  const auto shuffle = std::make_shared<Shuffle>(table_wrapper, std::vector<ColumnID>{ColumnID{0}},
                                                 std::vector<uint8_t>{PARTITION_COUNT});
  table_wrapper->execute();
  shuffle->never_clear_output();
  shuffle->execute();

  const auto result = shuffle->get_output();
  EXPECT_EQ(result->row_count(), table->row_count());
  EXPECT_EQ(result->chunk_count(), PARTITION_COUNT);

  auto all_values = std::unordered_set<int32_t>{};
  for (auto chunk_id = ChunkID{0}; chunk_id < result->chunk_count(); ++chunk_id) {
    const auto& chunk = result->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    const auto& segment = chunk->get_segment(ColumnID{0});
    auto segment_radix = std::optional<size_t>{};
    auto segment_values = std::unordered_set<int32_t>{};
    segment_iterate<int32_t>(*segment, [&](const auto& position) {
      EXPECT_FALSE(all_values.contains(position.value()));
      if (!segment_radix) {
        segment_radix = std::hash<int32_t>{}(position.value()) % PARTITION_COUNT;
      }
      EXPECT_EQ(std::hash<int32_t>{}(position.value()) % PARTITION_COUNT, *segment_radix);
      segment_values.emplace(position.value());
    });
    all_values.merge(segment_values);
  }
}

TEST_F(OperatorsShuffleTest, TwoColumns) {
  constexpr auto PARTITION_COUNT = uint8_t{8};
  constexpr auto ROW_COUNT = int32_t{64};
  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false},
                                                         {"b", DataType::Int, false}};
  const auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{4}, UseMvcc::Yes);

  for (auto value = int32_t{0}; value < ROW_COUNT; ++value) {
    table->append({value, ROW_COUNT - value});
  }

  Hyrise::get().storage_manager.add_table("table_a", table);

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  const auto shuffle = std::make_shared<Shuffle>(table_wrapper, std::vector{ColumnID{0}, ColumnID{1}},
                                                 std::vector{PARTITION_COUNT, PARTITION_COUNT});
  table_wrapper->execute();
  shuffle->never_clear_output();
  shuffle->execute();

  const auto result = shuffle->get_output();
  EXPECT_EQ(result->row_count(), table->row_count());
  EXPECT_EQ(result->chunk_count(), PARTITION_COUNT*PARTITION_COUNT);

  auto all_values = std::set<std::pair<int32_t, int32_t>>{};
  for (auto chunk_id = ChunkID{0}; chunk_id < result->chunk_count(); ++chunk_id) {
    const auto& chunk = result->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    const auto& left_segment = chunk->get_segment(ColumnID{0});
    const auto& right_segment = chunk->get_segment(ColumnID{1});

    auto segment_values = std::set<std::pair<int32_t, int32_t>>{};
    segment_iterate<int32_t>(*left_segment, [&](const auto& left_position) {
      segment_iterate<int32_t>(*right_segment, [&](const auto& right_position) {
        EXPECT_FALSE(all_values.contains({left_position.value(), right_position.value()}));
        segment_values.emplace(left_position.value(), right_position.value());
      });
    });
    all_values.merge(segment_values);
  }
}

// We are not going to go beyond three columns as the verification is extremely slow when using sets of vectors.
TEST_F(OperatorsShuffleTest, ThreeColumns) {
  constexpr auto PARTITION_COUNT = uint8_t{8};

  auto row_count = int32_t{1'000};
  if (!HYRISE_DEBUG) {
    row_count *= 10;
  }

  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false},
                                                         {"b", DataType::Int, false},
                                                         {"c", DataType::Int, false},
                                                         {"d", DataType::Int, false},
                                                         {"e", DataType::Int, false}};

  auto random_engine = std::default_random_engine();
  auto dist = std::uniform_int_distribution<int32_t>(1, 64);

  const auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{row_count / 100}, UseMvcc::Yes);

  for (auto value = int32_t{0}; value < row_count; ++value) {
    auto values = std::vector<AllTypeVariant>{};
    for (auto column_id = ColumnID{0}; column_id < 5; ++column_id) {
      values.emplace_back(dist(random_engine));
    }
    table->append(values);
  }

  Hyrise::get().storage_manager.add_table("table_a", table);

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  const auto shuffle = std::make_shared<Shuffle>(table_wrapper, std::vector{ColumnID{2}, ColumnID{1}, ColumnID{3}},
                                                 std::vector{PARTITION_COUNT, PARTITION_COUNT, PARTITION_COUNT});
  table_wrapper->execute();
  shuffle->never_clear_output();
  shuffle->execute();

  const auto result = shuffle->get_output();
  EXPECT_EQ(result->row_count(), table->row_count());
  EXPECT_EQ(result->chunk_count(), PARTITION_COUNT*PARTITION_COUNT*PARTITION_COUNT);

  auto all_values = std::unordered_set<std::array<int32_t, 3>>{};
  for (auto chunk_id = ChunkID{0}; chunk_id < result->chunk_count(); ++chunk_id) {
    const auto& chunk = result->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    const auto& first_segment = chunk->get_segment(ColumnID{1});
    const auto& second_segment = chunk->get_segment(ColumnID{2});
    const auto& third_segment = chunk->get_segment(ColumnID{3});

    auto segment_values = std::unordered_set<std::array<int32_t, 3>>{};
    segment_iterate<int32_t>(*first_segment, [&](const auto& first_position) {
      segment_iterate<int32_t>(*second_segment, [&](const auto& second_position) {
        segment_iterate<int32_t>(*third_segment, [&](const auto& third_position) {
          auto values = std::array<int32_t, 3>{first_position.value(), second_position.value(), third_position.value()};
          EXPECT_FALSE(all_values.contains(values));
          segment_values.emplace(values);
        });
      });
    });
    all_values.merge(segment_values);
  }
}

}  // namespace hyrise
