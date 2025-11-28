#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <random>
#include <string>

#include "benchmark/benchmark.h"

#include "all_type_variant.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace {

constexpr auto REFERENCED_TABLE_CHUNK_COUNT = hyrise::ChunkID{10};

// actually, for ease of implementation, the number of chunks will likely be GENERATED_TABLE_NUM_CHUNKS + 1
constexpr auto GENERATED_TABLE_NUM_CHUNKS = 4;

/**
 * Generate a random pos_list of length pos_list_size with ChunkIDs from [0,REFERENCED_TABLE_CHUNK_COUNT)
 * and ChunkOffsets within [0, std::floor(referenced_table_chunk_size))
 */
std::shared_ptr<hyrise::RowIDPosList> generate_pos_list(float referenced_table_chunk_size, size_t pos_list_size) {
  auto random_device = std::random_device{};
  auto random_engine = std::default_random_engine(random_device());

  auto chunk_id_distribution = std::uniform_int_distribution<hyrise::ChunkID::base_type>(
      0, static_cast<hyrise::ChunkID::base_type>(REFERENCED_TABLE_CHUNK_COUNT - 1));
  auto chunk_offset_distribution = std::uniform_int_distribution<hyrise::ChunkOffset::base_type>(
      hyrise::ChunkOffset{0}, static_cast<hyrise::ChunkOffset::base_type>(referenced_table_chunk_size - 1));

  auto pos_list = std::make_shared<hyrise::RowIDPosList>();
  pos_list->reserve(pos_list_size);

  for (auto pos_list_idx = size_t{0}; pos_list_idx < pos_list_size; ++pos_list_idx) {
    const auto chunk_id = hyrise::ChunkID{chunk_id_distribution(random_engine)};
    const auto chunk_offset = hyrise::ChunkOffset{chunk_offset_distribution(random_engine)};

    pos_list->emplace_back(chunk_id, chunk_offset);
  }

  return pos_list;
}

std::shared_ptr<Table> create_reference_table(const std::shared_ptr<Table>& referenced_table, size_t num_rows,
                                              size_t num_columns) {
  const auto num_rows_per_chunk = num_rows / GENERATED_TABLE_NUM_CHUNKS;

  auto column_definitions = TableColumnDefinitions{};
  for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
    column_definitions.emplace_back("c" + std::to_string(column_id), DataType::Int, false);
  }
  auto table = std::make_shared<Table>(column_definitions, TableType::References);

  for (auto row_id = ChunkOffset{0}; row_id < num_rows;) {
    const auto num_rows_in_this_chunk = std::min(num_rows_per_chunk, num_rows - row_id);

    auto segments = Segments{};
    for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
      /**
       * By specifying a chunk size of num_rows * 0.2f for the referenced table, we're emulating a referenced table
       * of (num_rows * 0.2f) * REFERENCED_TABLE_CHUNK_COUNT rows - i.e. twice as many rows as the referencing table
       * we're creating. So when creating TWO referencing tables, there should be a fair amount of overlap.
       */
      auto pos_list = generate_pos_list(static_cast<float>(num_rows) * 0.2f, num_rows_per_chunk);
      segments.push_back(std::make_shared<ReferenceSegment>(referenced_table, column_id, pos_list));
    }
    table->append_chunk(segments);

    row_id += num_rows_in_this_chunk;
  }

  return table;
}

void bm_union_positions(::benchmark::State& state) {
  const auto num_rows = 500000;
  const auto num_columns = 5;

  /**
   * Create the referenced table, that doesn't actually contain any data - but UnionPositions won't care, it just
   * operates on RowIDs
   */
  TableColumnDefinitions column_definitions;

  for (auto column_idx = 0; column_idx < num_columns; ++column_idx) {
    column_definitions.emplace_back("c" + std::to_string(column_idx), DataType::Int, false);
  }
  auto referenced_table = std::make_shared<Table>(column_definitions, TableType::Data);

  /**
   * Create the referencing tables, the ones we're actually going to perform the benchmark on
   */
  auto table_wrapper_left =
      std::make_shared<TableWrapper>(create_reference_table(referenced_table, num_rows, num_columns));
  table_wrapper_left->never_clear_output();
  table_wrapper_left->execute();
  auto table_wrapper_right =
      std::make_shared<TableWrapper>(create_reference_table(referenced_table, num_rows, num_columns));
  table_wrapper_right->never_clear_output();
  table_wrapper_right->execute();

  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    auto set_union = std::make_shared<UnionPositions>(table_wrapper_left, table_wrapper_right);
    set_union->execute();
  }
}

/**
 * Measure what sorting and merging two pos lists would cost - that's the core of the UnionPositions implementation and sets
 * a performance base line for what UnionPositions could achieve in an overhead-free implementation.
 */
void bm_union_positions_base_line(::benchmark::State& state) {
  auto num_table_rows = 500000;

  auto pos_list_left = generate_pos_list(static_cast<float>(num_table_rows) * 0.2f, num_table_rows);
  auto pos_list_right = generate_pos_list(static_cast<float>(num_table_rows) * 0.2f, num_table_rows);

  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    // Create copies, this would need to be done the UnionPositions Operator as well
    auto& left = *pos_list_left;
    auto& right = *pos_list_right;

    std::ranges::sort(left);
    std::ranges::sort(right);

    RowIDPosList result;
    result.reserve(std::min(left.size(), right.size()));
    std::ranges::set_union(left, right, std::back_inserter(result));
  }
}

}  // namespace

namespace hyrise {

BENCHMARK(bm_union_positions);

BENCHMARK(bm_union_positions_base_line);
}  // namespace hyrise
