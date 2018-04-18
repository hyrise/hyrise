#include <memory>

#include "../benchmark_join_fixture.hpp"
#include "../table_generator.hpp"
#include "benchmark/benchmark.h"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkJoinFixture, BM_JoinHash)(benchmark::State& state) {
  clear_cache();

  auto warm_up =
      std::make_shared<JoinHash>(_tw_small_uni1, _tw_small_uni2, JoinMode::Inner,
                                 std::pair<ColumnID, ColumnID>{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan =
        std::make_shared<JoinHash>(_tw_small_uni1, _tw_small_uni2, JoinMode::Inner,
                                   std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkJoinFixture, BM_JoinSortMerge)(benchmark::State& state) {
  clear_cache();

  auto warm_up = std::make_shared<JoinSortMerge>(_tw_small_uni1, _tw_small_uni2, JoinMode::Inner,
                                                 std::pair<ColumnID, ColumnID>{ColumnID{0}, ColumnID{0}},
                                                 PredicateCondition::Equals);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<JoinSortMerge>(_tw_small_uni1, _tw_small_uni2, JoinMode::Inner,
                                                      std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                                      PredicateCondition::Equals);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkJoinFixture, BM_JoinNestedLoop)(benchmark::State& state) {
  clear_cache();

  auto warm_up = std::make_shared<JoinNestedLoop>(_tw_small_uni1, _tw_small_uni2, JoinMode::Inner,
                                                  std::pair<ColumnID, ColumnID>{ColumnID{0}, ColumnID{0}},
                                                  PredicateCondition::Equals);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<JoinNestedLoop>(_tw_small_uni1, _tw_small_uni2, JoinMode::Inner,
                                                       std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                                       PredicateCondition::Equals);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkJoinFixture, BM_JoinIndex)(benchmark::State& state) {
  clear_cache();

  auto warm_up =
      std::make_shared<JoinIndex>(_tw_small_uni1, _tw_small_uni2, JoinMode::Inner,
                                  std::pair<ColumnID, ColumnID>{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<JoinIndex>(_tw_small_uni1, _tw_small_uni2, JoinMode::Inner,
                                                  std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                                  PredicateCondition::Equals);
    table_scan->execute();
  }
}

BENCHMARK_REGISTER_F(BenchmarkJoinFixture, BM_JoinSortMerge)
    ->Iterations(1)
    ->Apply(BenchmarkJoinFixture::ChunkSizeInUni);
BENCHMARK_REGISTER_F(BenchmarkJoinFixture, BM_JoinSortMerge)
    ->Iterations(1)
    ->Apply(BenchmarkJoinFixture::ChunkSizeInNormal);
BENCHMARK_REGISTER_F(BenchmarkJoinFixture, BM_JoinSortMerge)
    ->Iterations(1)
    ->Apply(BenchmarkJoinFixture::ChunkSizeInPareto);
BENCHMARK_REGISTER_F(BenchmarkJoinFixture, BM_JoinIndex)->Iterations(1)->Apply(BenchmarkJoinFixture::ChunkSizeInUni);
BENCHMARK_REGISTER_F(BenchmarkJoinFixture, BM_JoinIndex)->Iterations(1)->Apply(BenchmarkJoinFixture::ChunkSizeInNormal);
BENCHMARK_REGISTER_F(BenchmarkJoinFixture, BM_JoinIndex)->Iterations(1)->Apply(BenchmarkJoinFixture::ChunkSizeInPareto);

}  // namespace opossum
