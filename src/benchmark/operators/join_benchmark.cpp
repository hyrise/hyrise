#include <cstddef>
#include <memory>
#include <vector>

#include "benchmark/benchmark.h"

#include "hyrise.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/encoding_type.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {
constexpr auto NUMBER_OF_CHUNKS = size_t{50};

// These numbers were arbitrarily chosen to form a representative group of JoinBenchmarks
// that run in a tolerable amount of time
constexpr auto TABLE_SIZE_SMALL = size_t{1'000};
constexpr auto TABLE_SIZE_MEDIUM = size_t{100'000};
constexpr auto TABLE_SIZE_BIG = size_t{10'000'000};

void clear_cache() {
  auto clear = std::vector<int>();
  clear.resize(size_t{500} * 1000 * 1000, 42);
  const auto clear_cache_size = clear.size();
  for (auto index = size_t{0}; index < clear_cache_size; index++) {
    clear[index] += 1;
  }
  clear.resize(0);
}

std::shared_ptr<TableWrapper> generate_table(const size_t number_of_rows) {
  auto table_generator = std::make_shared<SyntheticTableGenerator>();

  const auto chunk_size = static_cast<ChunkOffset>(number_of_rows / NUMBER_OF_CHUNKS);
  Assert(chunk_size > 0, "The chunk size is 0 or less, cannot generate such a table.");

  auto table =
      table_generator->generate_table(1, number_of_rows, chunk_size, SegmentEncodingSpec{EncodingType::Dictionary});

  const auto chunk_count = table->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    for (auto column_id = ColumnID{0}; column_id < chunk->column_count(); ++column_id) {
      chunk->create_index<AdaptiveRadixTreeIndex>(std::vector<ColumnID>{column_id});
    }
  }

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->never_clear_output();
  table_wrapper->execute();

  return table_wrapper;
}

template <class C>
void bm_join_impl(benchmark::State& state, const std::shared_ptr<TableWrapper>& table_wrapper_left,
                  const std::shared_ptr<TableWrapper>& table_wrapper_right) {
  clear_cache();

  auto warm_up = std::make_shared<C>(table_wrapper_left, table_wrapper_right, JoinMode::Inner,
                                     OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  warm_up->execute();
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    auto join = std::make_shared<C>(table_wrapper_left, table_wrapper_right, JoinMode::Inner,
                                    OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
    join->execute();
  }

  Hyrise::reset();
}

template <class C>
void bm_join_small_and_small(benchmark::State& state) {  // 1,000 x 1,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_SMALL);
  auto table_wrapper_right = generate_table(TABLE_SIZE_SMALL);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

template <class C>
void bm_join_small_and_big(benchmark::State& state) {  // 1,000 x 10,000,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_SMALL);
  auto table_wrapper_right = generate_table(TABLE_SIZE_BIG);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

template <class C>
void bm_join_medium_and_medium(benchmark::State& state) {  // 100,000 x 100,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_MEDIUM);
  auto table_wrapper_right = generate_table(TABLE_SIZE_MEDIUM);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

}  // namespace

namespace hyrise {

BENCHMARK_TEMPLATE(bm_join_small_and_small, JoinNestedLoop);

BENCHMARK_TEMPLATE(bm_join_small_and_small, JoinIndex);
BENCHMARK_TEMPLATE(bm_join_small_and_big, JoinIndex);
BENCHMARK_TEMPLATE(bm_join_medium_and_medium, JoinIndex);

BENCHMARK_TEMPLATE(bm_join_small_and_small, JoinHash);
BENCHMARK_TEMPLATE(bm_join_small_and_big, JoinHash);
BENCHMARK_TEMPLATE(bm_join_medium_and_medium, JoinHash);

BENCHMARK_TEMPLATE(bm_join_small_and_small, JoinSortMerge);
BENCHMARK_TEMPLATE(bm_join_small_and_big, JoinSortMerge);
BENCHMARK_TEMPLATE(bm_join_medium_and_medium, JoinSortMerge);

}  // namespace hyrise
