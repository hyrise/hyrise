#include <algorithm>
#include <memory>
#include <random>
#include <utility>

#include "benchmark/benchmark.h"

#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace {

constexpr auto REFERENCED_TABLE_CHUNK_COUNT = opossum::ChunkID{10};

// actually, for ease of implementation, the number of chunks will likely be GENERATED_TABLE_NUM_CHUNKS + 1
constexpr auto GENERATED_TABLE_NUM_CHUNKS = 4;

/**
 * Generate a random pos_list of length pos_list_size with ChunkIDs from [0,REFERENCED_TABLE_CHUNK_COUNT)
 * and ChunkOffsets within [0, std::floor(referenced_table_chunk_size))
 */
std::shared_ptr<opossum::PosList> generate_pos_list(float referenced_table_chunk_size, size_t pos_list_size) {
  std::random_device random_device;
  std::default_random_engine random_engine(random_device());

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

std::shared_ptr<Table> create_reference_table(std::shared_ptr<Table> referenced_table, size_t num_rows,
                                              size_t num_columns) {
  const auto num_rows_per_chunk = num_rows / GENERATED_TABLE_NUM_CHUNKS;

  TableColumnDefinitions column_definitions;
  for (size_t column_idx = 0; column_idx < num_columns; ++column_idx) {
    column_definitions.emplace_back("c" + std::to_string(column_idx), DataType::Int, false);
  }
  auto table = std::make_shared<Table>(column_definitions, TableType::References);

  for (size_t row_idx = 0; row_idx < num_rows;) {
    const auto num_rows_in_this_chunk = std::min(num_rows_per_chunk, num_rows - row_idx);

    Segments segments;
    for (auto column_idx = ColumnID{0}; column_idx < num_columns; ++column_idx) {
      /**
       * By specifying a chunk size of num_rows * 0.2f for the referenced table, we're emulating a referenced table
       * of (num_rows * 0.2f) * REFERENCED_TABLE_CHUNK_COUNT rows - i.e. twice as many rows as the referencing table
       * we're creating. So when creating TWO referencing tables, there should be a fair amount of overlap.
       */
      auto pos_list = generate_pos_list(num_rows * 0.2f, num_rows_per_chunk);
      segments.push_back(std::make_shared<ReferenceSegment>(referenced_table, column_idx, pos_list));
    }
    table->append_chunk(segments);

    row_idx += num_rows_in_this_chunk;
  }

  return table;
}

void BM_UnionPositions(::benchmark::State& state) {  // NOLINT
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
  table_wrapper_left->execute();
  auto table_wrapper_right =
      std::make_shared<TableWrapper>(create_reference_table(referenced_table, num_rows, num_columns));
  table_wrapper_right->execute();

  for (auto _ : state) {
    auto set_union = std::make_shared<UnionPositions>(table_wrapper_left, table_wrapper_right);
    set_union->execute();
  }
}
BENCHMARK(BM_UnionPositions);

/**
 * Measure what sorting and merging two pos lists would cost - that's the core of the UnionPositions implementation and sets
 * a performance base line for what UnionPositions could achieve in an overhead-free implementation.
 */
void BM_UnionPositionsBaseLine(::benchmark::State& state) {  // NOLINT
  auto num_table_rows = 500000;

  auto pos_list_left = generate_pos_list(num_table_rows * 0.2f, num_table_rows);
  auto pos_list_right = generate_pos_list(num_table_rows * 0.2f, num_table_rows);

  for (auto _ : state) {
    // Create copies, this would need to be done the UnionPositions Operator as well
    auto& left = *pos_list_left;
    auto& right = *pos_list_right;

    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());

    PosList result;
    result.reserve(std::min(left.size(), right.size()));
    std::set_union(left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(result));
  }
}
BENCHMARK(BM_UnionPositionsBaseLine);
}  // namespace opossum
