#include <algorithm>
#include <array>
#include <memory>
#include <numeric>
#include <random>
#include <thread>

#include "benchmark/benchmark.h"

#include "cache/cache.hpp"
#include "cache/gdfs_cache.hpp"
#include "hyrise.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace opossum {

/**
 * Welcome to the benchmark playground. Here, you can quickly compare two
 * approaches in a minimal setup. Of course you can also use it to just benchmark
 * one single thing.
 *
 * In this example, a minimal TableScan-like operation is used to evaluate the
 * performance impact of pre-allocating the result vector (PosList in hyrise).
 *
 * A few tips:
 * * The optimizer is not your friend. If you do a bunch of calculations and
 *   don't actually use the result, it will optimize your code out and you will
 *   benchmark only noise.
 * * benchmark::DoNotOptimize(<expression>); marks <expression> as "globally
 *   aliased", meaning that the compiler has to assume that any operation that
 *   *could* access this memory location will do so.
 *   However, despite the name, this will not prevent the compiler from
 *   optimizing this expression itself!
 * * benchmark::ClobberMemory(); can be used to force calculations to be written
 *   to memory. It acts as a memory barrier. In combination with DoNotOptimize(e),
 *   this function effectively declares that it could touch any part of memory,
 *   in particular globally aliased memory.
 * * More information on that: https://stackoverflow.com/questions/40122141/
 */

using ValueT = int32_t;

class BenchmarkPlaygroundFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    MicroBenchmarkBasicFixture::SetUp(state);

    _clear_cache();
    const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}};
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

    pmr_vector<int32_t> values;
    values.resize(20'000);
    std::generate(_vec.begin(), _vec.end(), []() {
      static ValueT v = 0;
      v = (v + 1) % 10'000;
      return v;
    });

    auto segment = std::make_shared<ValueSegment<ValueT>>(std::move(values));
    Segments segment_vec;
    segment_vec.push_back(segment);

    const auto mvcc_data = std::make_shared<MvccData>(20'000, CommitID{0});
    table->append_chunk(segment_vec, mvcc_data);
    table->last_chunk()->finalize();
    Hyrise::get().storage_manager.add_table("foo", table);

    for (int32_t i = 0; i < 2 * _default_cache_size; i++) {
      _queries[i] = _sql_select + std::to_string(i) + ";";
    }
  }

  void TearDown(::benchmark::State& state) override { MicroBenchmarkBasicFixture::TearDown(state); }

 protected:
  std::vector<ValueT> _vec;
  std::array<std::string, 2 * 1024> _queries;
  const int32_t _small_cache_size = 10;
  const int32_t _default_cache_size = 1024;
  const std::string _sql_select = "SELECT * FROM foo WHERE a = ";
};

BENCHMARK_F(BenchmarkPlaygroundFixture, SimulateCacheUsageOldSmall)(benchmark::State& state) {
  std::mt19937 generator(1337);
  std::uniform_int_distribution<int32_t> dist(0, 2 * _small_cache_size);
  auto pqp_cache = std::make_shared<SQLPhysicalPlanCacheOld>(_small_cache_size);

  for (auto _ : state) {
    const int32_t value = dist(generator);
    const auto sql_string = _queries[value];
    bool cache_hit = false;
    std::shared_ptr<AbstractOperator> physical_plan;

    // Following code is an abbreviated copy from SQLPipelineStatement
    if (const auto cached_physical_plan = pqp_cache->try_get(sql_string)) {
      cache_hit = true;
    }

    // Cache newly created plan for the according sql statement (only if not already cached)
    if (!cache_hit) {
      pqp_cache->set(sql_string, physical_plan);
    }

    benchmark::ClobberMemory();
  }
}

BENCHMARK_F(BenchmarkPlaygroundFixture, SimulateCacheUsageNewSmall)(benchmark::State& state) {
  std::mt19937 generator(1337);
  std::uniform_int_distribution<int32_t> dist(0, 2 * _small_cache_size);
  auto pqp_cache = std::make_shared<SQLPhysicalPlanCache>(_small_cache_size);

  for (auto _ : state) {
    const int32_t value = dist(generator);
    const auto sql_string = _queries[value];
    bool cache_hit = false;
    std::shared_ptr<AbstractOperator> physical_plan;

    // Following code is an abbreviated copy from SQLPipelineStatement
    if (const auto cached_physical_plan = pqp_cache->try_get(sql_string)) {
      cache_hit = true;
    }

    // Cache newly created plan for the according sql statement (only if not already cached)
    if (!cache_hit) {
      pqp_cache->set(sql_string, physical_plan);
    }

    benchmark::ClobberMemory();
  }
}

BENCHMARK_F(BenchmarkPlaygroundFixture, SimulateCacheUsageOld)(benchmark::State& state) {
  std::cout << "SimulateCacheUsageOld" << std::endl;
  std::mt19937 generator(1337);
  std::uniform_int_distribution<int32_t> dist(0, 2 * _default_cache_size);
  auto pqp_cache = std::make_shared<SQLPhysicalPlanCacheOld>();

  for (auto _ : state) {
    const int32_t value = dist(generator);
    const auto sql_string = _queries[value];
    bool cache_hit = false;
    std::shared_ptr<AbstractOperator> physical_plan;

    // Following code is an abbreviated copy from SQLPipelineStatement
    if (const auto cached_physical_plan = pqp_cache->try_get(sql_string)) {
      cache_hit = true;
    }

    // Cache newly created plan for the according sql statement (only if not already cached)
    if (!cache_hit) {
      pqp_cache->set(sql_string, physical_plan);
    }

    benchmark::ClobberMemory();
  }
}

BENCHMARK_F(BenchmarkPlaygroundFixture, SimulateCacheUsageNew)(benchmark::State& state) {
  std::cout << "SimulateCacheUsageNew" << std::endl;
  std::mt19937 generator(1337);
  std::uniform_int_distribution<int32_t> dist(0, 2 * _default_cache_size);
  auto pqp_cache = std::make_shared<SQLPhysicalPlanCache>();

  for (auto _ : state) {
    const int32_t value = dist(generator);
    const auto sql_string = _queries[value];
    bool cache_hit = false;
    std::shared_ptr<AbstractOperator> physical_plan;

    // Following code is an abbreviated copy from SQLPipelineStatement
    if (const auto cached_physical_plan = pqp_cache->try_get(sql_string)) {
      cache_hit = true;
    }

    // Cache newly created plan for the according sql statement (only if not already cached)
    if (!cache_hit) {
      pqp_cache->set(sql_string, physical_plan);
    }

    benchmark::ClobberMemory();
  }
}

BENCHMARK_F(BenchmarkPlaygroundFixture, RandomSelect)(benchmark::State& state) {
  std::cout << "RandomSelect" << std::endl;
  std::mt19937 generator(1337);
  std::uniform_int_distribution<int32_t> dist(0, 2 * _default_cache_size);
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  for (auto _ : state) {
    const int32_t value = dist(generator);
    const auto sql_string = _queries[value];
    auto builder = SQLPipelineBuilder{sql_string};
    auto sql_pipeline = std::make_unique<SQLPipeline>(builder.create_pipeline());
    size_t result;
    benchmark::DoNotOptimize(result);
    const auto [pipeline_status, table] = sql_pipeline->get_result_table();
    result = table->row_count();
    benchmark::ClobberMemory();
  }
}

BENCHMARK_F(BenchmarkPlaygroundFixture, RandomSelectWithSnap)(benchmark::State& state) {
  std::cout << "RandomSelectWithSnap" << std::endl;
  std::mt19937 generator(1337);
  std::uniform_int_distribution<int32_t> dist(0, 2 * _default_cache_size);
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  std::unique_ptr<PausableLoopThread> loop_thread_snap =
      std::make_unique<PausableLoopThread>(std::chrono::seconds(5), [&](size_t) {
        size_t cached_items;
        benchmark::DoNotOptimize(cached_items);
        cached_items = Hyrise::get().default_pqp_cache->snapshot().size();
        benchmark::ClobberMemory();
      });

  for (auto _ : state) {
    const int32_t value = dist(generator);
    const auto sql_string = _queries[value];
    auto builder = SQLPipelineBuilder{sql_string};
    auto sql_pipeline = std::make_unique<SQLPipeline>(builder.create_pipeline());
    size_t result;
    benchmark::DoNotOptimize(result);
    const auto [pipeline_status, table] = sql_pipeline->get_result_table();
    result = table->row_count();
    benchmark::ClobberMemory();
  }

  loop_thread_snap.reset();
}

}  // namespace opossum
