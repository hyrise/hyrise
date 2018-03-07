#include <algorithm>
#include <memory>
#include <random>
#include <utility>

#include "benchmark/benchmark.h"

#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace {

constexpr auto REFERENCED_TABLE_CHUNK_COUNT = opossum::ChunkID{10};

// actually, for ease of implementation, the number of chunks will likely be GENERATED_TABLE_NUM_CHUNKS + 1
constexpr auto GENERATED_TABLE_NUM_CHUNKS = 4;

/**
 * Generate a random pos_list of length std::floor(pos_list_size) with ChunkIDs from [0,REFERENCED_TABLE_CHUNK_COUNT)
 * and ChunkOffsets within [0, std::floor(referenced_table_chunk_size))
 */
std::shared_ptr<opossum::PosList> generate_pos_list(std::default_random_engine& random_engine,
                                                    float referenced_table_chunk_size, float pos_list_size) {
  std::uniform_int_distribution<opossum::ChunkID::base_type> chunk_id_distribution(
      0, static_cast<opossum::ChunkID::base_type>(REFERENCED_TABLE_CHUNK_COUNT - 1));
  std::uniform_int_distribution<opossum::ChunkOffset> chunk_offset_distribution(
      opossum::ChunkOffset{0}, static_cast<opossum::ChunkOffset>(referenced_table_chunk_size - 1));

  auto pos_list = std::make_shared<opossum::PosList>();
  pos_list->reserve(pos_list_size);

  for (size_t pos_list_idx = 0; pos_list_idx < pos_list_size; ++pos_list_idx) {
    const auto chunk_id = opossum::ChunkID{chunk_id_distribution(random_engine)};
    const auto chunk_offset = chunk_offset_distribution(random_engine);

    pos_list->emplace_back(opossum::RowID{chunk_id, chunk_offset});
  }

  return pos_list;
}
}  // namespace

namespace opossum {

class UnionPositionsBenchmarkFixture : public benchmark::Fixture {
 public:
  UnionPositionsBenchmarkFixture() : _random_device(), _random_engine(_random_device()) {}

  void SetUp(::benchmark::State& state) override {
    const auto num_rows = state.range(0);
    const auto num_columns = state.range(1);

    /**
     * Create the referenced table, that doesn't actually contain any data - but UnionPositions won't care, it just
     * operates on RowIDs
     */
    TableColumnDefinitions column_definitions;

    for (auto column_idx = 0; column_idx < num_columns; ++column_idx) {
      column_definitions.emplace_back("c" + std::to_string(column_idx), DataType::Int);
    }
    _referenced_table = std::make_shared<Table>(column_definitions, TableType::Data);

    /**
     * Create the referencing tables, the ones we're actually going to perform the benchmark on
     */
    _table_wrapper_left = std::make_shared<TableWrapper>(_create_reference_table(num_rows, num_columns));
    _table_wrapper_left->execute();
    _table_wrapper_right = std::make_shared<TableWrapper>(_create_reference_table(num_rows, num_columns));
    _table_wrapper_right->execute();
  }

 protected:
  std::random_device _random_device;
  mutable std::default_random_engine _random_engine;
  std::shared_ptr<TableWrapper> _table_wrapper_left;
  std::shared_ptr<TableWrapper> _table_wrapper_right;
  std::shared_ptr<Table> _referenced_table;

  std::shared_ptr<Table> _create_reference_table(size_t num_rows, size_t num_columns) const {
    const auto num_rows_per_chunk = num_rows / GENERATED_TABLE_NUM_CHUNKS;

    TableColumnDefinitions column_definitions;
    for (size_t column_idx = 0; column_idx < num_columns; ++column_idx) {
      column_definitions.emplace_back("c" + std::to_string(column_idx), DataType::Int);
    }
    auto table = std::make_shared<Table>(column_definitions, TableType::References);

    for (size_t row_idx = 0; row_idx < num_rows;) {
      const auto num_rows_in_this_chunk = std::min(num_rows_per_chunk, num_rows - row_idx);

      ChunkColumns columns;
      for (auto column_idx = ColumnID{0}; column_idx < num_columns; ++column_idx) {
        /**
         * By specifying a chunk size of num_rows * 0.2f for the referenced table, we're emulating a referenced table
         * of (num_rows * 0.2f) * REFERENCED_TABLE_CHUNK_COUNT rows - i.e. twice as many rows as the referencing table
         * we're creating. So when creating TWO referencing tables, there should be a fair amount of overlap.
         */
        auto pos_list = generate_pos_list(_random_engine, num_rows * 0.2f, num_rows_per_chunk);
        columns.push_back(std::make_shared<ReferenceColumn>(_referenced_table, column_idx, pos_list));
      }
      table->append_chunk(columns);

      row_idx += num_rows_in_this_chunk;
    }

    return table;
  }
};

BENCHMARK_DEFINE_F(UnionPositionsBenchmarkFixture, Benchmark)(::benchmark::State& state) {
  while (state.KeepRunning()) {
    auto set_union = std::make_shared<UnionPositions>(_table_wrapper_left, _table_wrapper_right);
    set_union->execute();
  }
}
BENCHMARK_REGISTER_F(UnionPositionsBenchmarkFixture, Benchmark)->Ranges({{100, 5 * 1000 * 1000}, {1, 4}});

/**
 * Measure what sorting and merging two pos lists would cost - that's the core of the UnionPositions implementation and sets
 * a performance base line for what UnionPositions could achieve in an overhead-free implementation.
 */
class UnionPositionsBaseLineBenchmarkFixture : public benchmark::Fixture {
 public:
  UnionPositionsBaseLineBenchmarkFixture() : _random_device(), _random_engine(_random_device()) {}

  void SetUp(::benchmark::State& state) override {
    auto num_table_rows = state.range(0);

    _pos_list_left = generate_pos_list(_random_engine, num_table_rows * 0.2f, num_table_rows);
    _pos_list_right = generate_pos_list(_random_engine, num_table_rows * 0.2f, num_table_rows);
  }

 protected:
  std::random_device _random_device;
  std::default_random_engine _random_engine;
  std::shared_ptr<PosList> _pos_list_left;
  std::shared_ptr<PosList> _pos_list_right;
};

BENCHMARK_DEFINE_F(UnionPositionsBaseLineBenchmarkFixture, Benchmark)(::benchmark::State& state) {
  while (state.KeepRunning()) {
    // Create copies, this would need to be done the UnionPositions Operator as well
    auto left = *_pos_list_left;
    auto right = *_pos_list_right;

    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());

    PosList result;
    result.reserve(std::min(left.size(), right.size()));
    std::set_union(left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(result));
  }
}
BENCHMARK_REGISTER_F(UnionPositionsBaseLineBenchmarkFixture, Benchmark)->Range(100, 5 * 1000 * 1000);
}  // namespace opossum
