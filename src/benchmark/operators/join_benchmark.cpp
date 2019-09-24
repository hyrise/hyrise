#include <memory>

#include "micro_benchmark_basic_fixture.hpp"

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
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {
using namespace opossum;

constexpr auto NUMBER_OF_CHUNKS = size_t{50};

constexpr auto ROW_COUNTS = {size_t{10'000}, size_t{100'000}};
constexpr auto CHUNK_SIZES = {ChunkOffset{10'000}, ChunkOffset{100'000}};

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
BENCHMARK(BM_JoinInequality)->RangeMultiplier(2)->Ranges({{2<<16, 2<<20}, {1, 256}, {true, false}});

static void BM_JoinInequalityStatic(benchmark::State& state) {
  // Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  const auto scale_factor = 1'000'000;
  const auto cluster_count = 32;
  const auto radix_clustering = state.range(0);
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

  // Hyrise::get().scheduler().finish();
}
BENCHMARK(BM_JoinInequalityStatic)->RangeMultiplier(2)->Ranges({{true, false}});

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

class JoinBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) {
    auto& storage_manager = Hyrise::get().storage_manager;

    if (!_data_generated) {
      auto table_generator = std::make_shared<SyntheticTableGenerator>();

      auto distribution_left = ColumnDataDistribution::make_uniform_config(0.0, 10'000);
      auto distribution_right = ColumnDataDistribution::make_uniform_config(0.0, 30'000);

      const auto encoding_spec = std::vector<SegmentEncodingSpec>({SegmentEncodingSpec{EncodingType::Dictionary}});

      std::cout << "Generating tables." << std::endl;
      for (const auto row_count : ROW_COUNTS) {
        for (const auto chunk_size : CHUNK_SIZES) {
          auto table_l = table_generator->generate_table({distribution_left}, {DataType::Int}, row_count, chunk_size, encoding_spec, std::nullopt, UseMvcc::Yes);
          storage_manager.add_table("left__rc_" + std::to_string(row_count) + "_cz_" + std::to_string(chunk_size), table_l);

          auto table_r = table_generator->generate_table({distribution_right}, {DataType::Int}, row_count * 10, chunk_size, encoding_spec, std::nullopt, UseMvcc::Yes);
          storage_manager.add_table("right__rc_" + std::to_string(row_count*10) + "_cz_" + std::to_string(chunk_size), table_r);

          std::cout << "Added tables for row count " << row_count << " and chunk size " << chunk_size << "." << std::endl;
        }
      }

      _data_generated = true;
    }
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State&) {}

  inline static bool _data_generated = false;

  std::shared_ptr<Table> t;
};

BENCHMARK_DEFINE_F(JoinBenchmarkFixture, BM_JoinInequalityVarying)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;

  const auto row_count = state.range(0);
  const auto chunk_size = state.range(1);
  const auto cluster_count = state.range(2);
  const auto radix_clustering = state.range(3);

  const auto left_table_name = "left__rc_" + std::to_string(row_count) + "_cz_" + std::to_string(chunk_size);
  const auto right_table_name = "right__rc_" + std::to_string(row_count*10) + "_cz_" + std::to_string(chunk_size);

  auto table_left = storage_manager.get_table(left_table_name);
  auto table_wrapper_left = std::make_shared<TableWrapper>(table_left);
  table_wrapper_left->execute();
  auto table_right = storage_manager.get_table(right_table_name);
  auto table_wrapper_right = std::make_shared<TableWrapper>(table_right);
  table_wrapper_right->execute();

  auto expression_left = PQPColumnExpression::from_table(*table_left, "column_1");
  auto expression_right = PQPColumnExpression::from_table(*table_right, "column_1");
  auto table_scan_left = std::make_shared<TableScan>(table_wrapper_left, greater_than_equals_(expression_left, 1'000));  // 0.9 selectivity
  auto table_scan_right = std::make_shared<TableScan>(table_wrapper_right, greater_than_equals_(expression_right, 3'000));  // 0.9 selectivity
  table_scan_left->execute();
  table_scan_right->execute();

  for (auto _ : state) {
    auto clusterer = JoinSortMergeClusterer<int>(
        table_scan_left->get_output(), table_scan_right->get_output(), ColumnIDPair{0, 0}, radix_clustering,
        false, false, cluster_count);
    clusterer.execute();
  }
}

static void CustomArguments(benchmark::internal::Benchmark* b) {
  for (const auto row_count : ROW_COUNTS) {
    for (const auto chunk_size : CHUNK_SIZES) {
      for (const auto cluster_count : {1, 2, 8, 16, 32, 64}) {
        for (const auto radix_clustering : {true, false}) {
          b->Args({static_cast<long long>(row_count), static_cast<long long>(chunk_size), cluster_count, radix_clustering});
        }
      }
    }
  }
}
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, BM_JoinInequalityVarying)->Apply(CustomArguments);

}  // namespace opossum
