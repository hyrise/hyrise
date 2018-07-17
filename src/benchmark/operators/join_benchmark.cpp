#include <memory>

#include "benchmark/benchmark.h"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_mpsm.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"

namespace {
constexpr auto NUMBER_OF_CHUNKS = size_t{50};

// These numbers were arbitrarily chosen to form a representative group of JoinBenchmarks
// that run in a tolerable amount of time
constexpr auto TABLE_SIZE_SMALL = size_t{1000};
constexpr auto TABLE_SIZE_MEDIUM = size_t{100000};
constexpr auto TABLE_SIZE_BIG = size_t{10000000};
}  // namespace

namespace opossum {

void clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

std::shared_ptr<TableWrapper> generate_table(const size_t number_of_rows) {
  auto table_generator = std::make_shared<TableGenerator>();

  ColumnDataDistribution config = ColumnDataDistribution::make_uniform_config(0.0, 10000);
  const auto chunk_size = static_cast<ChunkID>(number_of_rows / NUMBER_OF_CHUNKS);
  Assert(chunk_size > 0, "The chunk size is 0 or less, can not generate such a table");

  auto table = table_generator->generate_table(std::vector<ColumnDataDistribution>{config}, number_of_rows, chunk_size,
                                               EncodingType::Dictionary);

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);

    std::vector<ColumnID> columns{1};
    for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
      columns[0] = column_id;
      chunk->create_index<AdaptiveRadixTreeIndex>(columns);
    }
  }

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  return table_wrapper;
}

template <class C>
void bm_join_impl(benchmark::State& state, std::shared_ptr<TableWrapper> table_wrapper_left,
                  std::shared_ptr<TableWrapper> table_wrapper_right) {
  clear_cache();

  auto warm_up =
      std::make_shared<C>(table_wrapper_left, table_wrapper_right, JoinMode::Inner,
                          std::pair<ColumnID, ColumnID>{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto join =
        std::make_shared<C>(table_wrapper_left, table_wrapper_right, JoinMode::Inner,
                            std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }

  opossum::StorageManager::get().reset();
}

template <class C>
void BM_Join_Small(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_SMALL);
  auto table_wrapper_right = generate_table(TABLE_SIZE_SMALL);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

template <class C>
void BM_Join_Skewed(benchmark::State& state) {  // NOLINT 1,000 x 10,000,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_SMALL);
  auto table_wrapper_right = generate_table(TABLE_SIZE_BIG);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

template <class C>
void BM_Join_Big(benchmark::State& state) {  // NOLINT 100,000 x 100,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_MEDIUM);
  auto table_wrapper_right = generate_table(TABLE_SIZE_MEDIUM);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

BENCHMARK_TEMPLATE(BM_Join_Small, JoinNestedLoop);

BENCHMARK_TEMPLATE(BM_Join_Small, JoinIndex);
BENCHMARK_TEMPLATE(BM_Join_Skewed, JoinIndex);
BENCHMARK_TEMPLATE(BM_Join_Big, JoinIndex);

BENCHMARK_TEMPLATE(BM_Join_Small, JoinHash);
BENCHMARK_TEMPLATE(BM_Join_Skewed, JoinHash);
BENCHMARK_TEMPLATE(BM_Join_Big, JoinHash);

BENCHMARK_TEMPLATE(BM_Join_Small, JoinSortMerge);
BENCHMARK_TEMPLATE(BM_Join_Skewed, JoinSortMerge);
BENCHMARK_TEMPLATE(BM_Join_Big, JoinSortMerge);

BENCHMARK_TEMPLATE(BM_Join_Small, JoinMPSM);
BENCHMARK_TEMPLATE(BM_Join_Skewed, JoinMPSM);
BENCHMARK_TEMPLATE(BM_Join_Big, JoinMPSM);

}  // namespace opossum
