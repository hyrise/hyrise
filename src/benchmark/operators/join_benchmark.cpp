#include <memory>

#include "benchmark/benchmark.h"
#include "hyrise.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_mpsm.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {
constexpr auto NUMBER_OF_CHUNKS = size_t{50};

// These numbers were arbitrarily chosen to form a representative group of JoinBenchmarks
// that run in a tolerable amount of time
constexpr auto TABLE_SIZE_SMALL = size_t{1'000};
constexpr auto TABLE_SIZE_MEDIUM = size_t{100'000};
constexpr auto TABLE_SIZE_BIG = size_t{10'000'000};

void clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}
}  // namespace

namespace opossum {

std::shared_ptr<TableWrapper> generate_table(const size_t number_of_rows) {
  auto table_generator = std::make_shared<SyntheticTableGenerator>();

  const auto chunk_size = static_cast<ChunkOffset>(number_of_rows / NUMBER_OF_CHUNKS);
  Assert(chunk_size > 0, "The chunk size is 0 or less, can not generate such a table");

  auto table =
      table_generator->generate_table(1ul, number_of_rows, chunk_size, SegmentEncodingSpec{EncodingType::Dictionary});

  const auto chunk_count = table->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
      chunk->create_index<AdaptiveRadixTreeIndex>(std::vector<ColumnID>{column_id});
    }
  }

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  return table_wrapper;
}

static void BM_JoinInequality(benchmark::State& state) {
  const auto scale_factor = state.range(0);
  const auto cluster_count = state.range(1);
  const auto radix_clustering = state.range(2);
  auto table_generator = std::make_shared<SyntheticTableGenerator>();

  auto distribution_left = ColumnDataDistribution::make_uniform_config(0.0, 10'000);
  auto distribution_right = ColumnDataDistribution::make_uniform_config(0.0, 30'000);

  const auto encoding_spec = std::vector<SegmentEncodingSpec>({SegmentEncodingSpec{EncodingType::Dictionary}});

  auto table_l = table_generator->generate_table({distribution_left}, {DataType::Int}, scale_factor, 100'000, encoding_spec);
  auto table_r = table_generator->generate_table({distribution_right}, {DataType::Int}, scale_factor * 10, 100'000, encoding_spec);

  auto table_wrapper_left = std::make_shared<TableWrapper>(table_l);
  table_wrapper_left->execute();
  auto table_wrapper_right = std::make_shared<TableWrapper>(table_r);
  table_wrapper_right->execute();

  auto expression_left = PQPColumnExpression::from_table(*table_l, "column_1");
  auto expression_right = PQPColumnExpression::from_table(*table_r, "column_1");
  auto table_scan_left = std::make_shared<TableScan>(table_wrapper_left, greater_than_equals_(expression_left, 1'000));
  auto table_scan_right = std::make_shared<TableScan>(table_wrapper_right, greater_than_equals_(expression_right, 3'000));
  table_scan_left->execute();
  table_scan_right->execute();

  for (auto _ : state) {
    auto clusterer = JoinSortMergeClusterer<int>(
        table_scan_left->get_output(), table_scan_right->get_output(), ColumnIDPair{0, 0}, radix_clustering,
        false, false, cluster_count);
    clusterer.execute();
  }
}
// BENCHMARK(BM_JoinInequality)->RangeMultiplier(2)->Ranges({{2<<10, 2<<23}, {1, 16}});
BENCHMARK(BM_JoinInequality)->RangeMultiplier(2)->Ranges({{2<<18, 2<<25}, {1, 512}, {true, false}});

template <class C>
void bm_join_impl(benchmark::State& state, std::shared_ptr<TableWrapper> table_wrapper_left,
                  std::shared_ptr<TableWrapper> table_wrapper_right) {
  clear_cache();

  auto warm_up = std::make_shared<C>(table_wrapper_left, table_wrapper_right, JoinMode::Inner,
                                     OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  warm_up->execute();
  for (auto _ : state) {
    auto join = std::make_shared<C>(table_wrapper_left, table_wrapper_right, JoinMode::Inner,
                                    OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
    join->execute();
  }

  opossum::Hyrise::reset();
}

template <class C>
void BM_Join_SmallAndSmall(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_SMALL);
  auto table_wrapper_right = generate_table(TABLE_SIZE_SMALL);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

template <class C>
void BM_Join_SmallAndBig(benchmark::State& state) {  // NOLINT 1,000 x 10,000,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_SMALL);
  auto table_wrapper_right = generate_table(TABLE_SIZE_BIG);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

template <class C>
void BM_Join_MediumAndMedium(benchmark::State& state) {  // NOLINT 100,000 x 100,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_MEDIUM);
  auto table_wrapper_right = generate_table(TABLE_SIZE_MEDIUM);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

BENCHMARK_TEMPLATE(BM_Join_SmallAndSmall, JoinNestedLoop);

BENCHMARK_TEMPLATE(BM_Join_SmallAndSmall, JoinIndex);
BENCHMARK_TEMPLATE(BM_Join_SmallAndBig, JoinIndex);
BENCHMARK_TEMPLATE(BM_Join_MediumAndMedium, JoinIndex);

BENCHMARK_TEMPLATE(BM_Join_SmallAndSmall, JoinHash);
BENCHMARK_TEMPLATE(BM_Join_SmallAndBig, JoinHash);
BENCHMARK_TEMPLATE(BM_Join_MediumAndMedium, JoinHash);

BENCHMARK_TEMPLATE(BM_Join_SmallAndSmall, JoinSortMerge);
BENCHMARK_TEMPLATE(BM_Join_SmallAndBig, JoinSortMerge);
BENCHMARK_TEMPLATE(BM_Join_MediumAndMedium, JoinSortMerge);

BENCHMARK_TEMPLATE(BM_Join_SmallAndSmall, JoinMPSM);
BENCHMARK_TEMPLATE(BM_Join_SmallAndBig, JoinMPSM);
BENCHMARK_TEMPLATE(BM_Join_MediumAndMedium, JoinMPSM);

}  // namespace opossum
