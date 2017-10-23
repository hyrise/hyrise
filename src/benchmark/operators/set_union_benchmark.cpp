#include <memory>
#include <random>
#include <utility>

#include "benchmark/benchmark.h"

#include "operators/set_union.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace {

std::shared_ptr<opossum::PosList> generate_pos_list(opossum::ChunkID referenced_table_chunk_count,
                                                    float referenced_table_chunk_size, float pos_list_size) {
  std::random_device _random_device;
  std::default_random_engine _random_engine;

  std::uniform_int_distribution<opossum::ChunkID::base_type> chunk_id_distribution(
      0, static_cast<opossum::ChunkID::base_type>(referenced_table_chunk_count - 1));
  std::uniform_int_distribution<opossum::ChunkOffset> chunk_offset_distribution(
      opossum::ChunkOffset{0}, static_cast<opossum::ChunkOffset>(referenced_table_chunk_size - 1));

  auto pos_list = std::make_shared<opossum::PosList>();
  pos_list->reserve(pos_list_size);

  for (size_t pos_list_idx = 0; pos_list_idx < pos_list_size; ++pos_list_idx) {
    const auto chunk_id = opossum::ChunkID{chunk_id_distribution(_random_engine)};
    const auto chunk_offset = chunk_offset_distribution(_random_engine);

    pos_list->emplace_back(opossum::RowID{chunk_id, chunk_offset});
  }

  return pos_list;
}
}

namespace opossum {

class SetUnionBenchmarkFixture : public benchmark::Fixture {
 public:
  void SetUp(::benchmark::State& state) override {
    const auto num_table_rows = state.range(0);
    const auto num_table_columns = state.range(1);

    auto table = std::make_shared<Table>();
    Chunk mock_chunk;

    for (auto column_idx = 0; column_idx < num_table_columns; ++column_idx) {
      // Create a pos list 60% of the length of the referenced table
      auto pos_list = generate_pos_list(ChunkID{10}, num_table_rows * 0.1f, num_table_rows * 0.6f);

      /**
       * Each row references its own table, that doesn't actually contain data. But SetUnion won't care, it just
       * operates on RowIDs
       */
      auto mock_table = std::make_shared<Table>();
      mock_table->add_column("dummy", "int");

      mock_chunk.add_column(std::make_shared<ReferenceColumn>(mock_table, ColumnID{0}, pos_list));
      table->add_column_definition("c" + std::to_string(column_idx), "int");
    }
    table->emplace_chunk(std::move(mock_chunk));

    _reference_table_wrapper = std::make_shared<TableWrapper>(table);
    _reference_table_wrapper->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _reference_table_wrapper;
};

BENCHMARK_DEFINE_F(SetUnionBenchmarkFixture, Benchmark)(::benchmark::State& state) {
  while (state.KeepRunning()) {
    auto set_union = std::make_shared<SetUnion>(_reference_table_wrapper, _reference_table_wrapper);
    set_union->execute();
  }
}
BENCHMARK_REGISTER_F(SetUnionBenchmarkFixture, Benchmark)->Ranges({{100, 100 * 1000 * 1000}, {1, 4}});

/**
 * Measure what sorting and merging two pos lists would cost
 */
class SetUnionBaseLineBenchmarkFixture : public benchmark::Fixture {
 public:
  void SetUp(::benchmark::State& state) override {
    auto num_table_rows = state.range(0);

    _pos_list_left = generate_pos_list(ChunkID{10}, num_table_rows * 0.1f, num_table_rows * 0.6f);
    _pos_list_right = generate_pos_list(ChunkID{10}, num_table_rows * 0.1f, num_table_rows * 0.6f);
  }

 protected:
  std::shared_ptr<PosList> _pos_list_left;
  std::shared_ptr<PosList> _pos_list_right;
};

BENCHMARK_DEFINE_F(SetUnionBaseLineBenchmarkFixture, Benchmark)(::benchmark::State& state) {
  while (state.KeepRunning()) {
    // Create copies, this would need to be done the SetUnion Operator as well
    auto left = *_pos_list_left;
    auto right = *_pos_list_right;

    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());

    PosList result;
    result.reserve(std::min(left.size(), right.size()));
    std::set_union(left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(result));
  }
}
BENCHMARK_REGISTER_F(SetUnionBaseLineBenchmarkFixture, Benchmark)->Range(100, 100 * 1000 * 1000);
}