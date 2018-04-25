#include <memory>

#include "../benchmark_basic_fixture.hpp"
#include "../table_generator.hpp"
#include "benchmark/benchmark.h"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanConstant)(benchmark::State& state) {
  clear_cache();

  auto warm_up =
      std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */, PredicateCondition::GreaterThanEquals, 7);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan =
        std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */, PredicateCondition::GreaterThanEquals, 7);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanConstantOnDict)(benchmark::State& state) {
  clear_cache();
  auto warm_up =
      std::make_shared<TableScan>(_table_dict_wrapper, ColumnID{0} /* "a" */, PredicateCondition::GreaterThanEquals, 7);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(_table_dict_wrapper, ColumnID{0} /* "a" */,
                                                  PredicateCondition::GreaterThanEquals, 7);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanVariable)(benchmark::State& state) {
  clear_cache();
  auto warm_up =
      std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, ColumnID{1});
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */,
                                                  PredicateCondition::GreaterThanEquals, ColumnID{1} /* "b" */);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanVariableOnDict)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<TableScan>(_table_dict_wrapper, ColumnID{0} /* "a" */,
                                             PredicateCondition::GreaterThanEquals, ColumnID{1} /* "b" */);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(_table_dict_wrapper, ColumnID{0} /* "a" */,
                                                  PredicateCondition::GreaterThanEquals, ColumnID{1} /* "b" */);
    table_scan->execute();
  }
}

void BM_TableScanLike(benchmark::State& state, const std::string& column_name, const std::string& pattern) {
  const auto lineitem_table = load_table("src/test/tables/tpch/sf-0.001/lineitem.tbl");

  const auto lineitem_wrapper = std::make_shared<TableWrapper>(lineitem_table);
  lineitem_wrapper->execute();

  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(lineitem_wrapper, lineitem_table->column_id_by_name(column_name),
                                                  PredicateCondition::Like, pattern);
    table_scan->execute();
  }
}

BENCHMARK_CAPTURE(BM_TableScanLike, FindPattern, "l_comment", "%final%");
BENCHMARK_CAPTURE(BM_TableScanLike, MultipleFindPatterns, "l_comment", "%final%requests%");
BENCHMARK_CAPTURE(BM_TableScanLike, StartsWithPattern, "l_shipinstruct", "quickly%");
BENCHMARK_CAPTURE(BM_TableScanLike, EndsWithPattern, "l_comment", "%foxes");
BENCHMARK_CAPTURE(BM_TableScanLike, ComplexPattern, "l_comment", "%quick_y__above%even%");

BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanConstant)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanConstantOnDict)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanVariable)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanVariableOnDict)->Apply(BenchmarkBasicFixture::ChunkSizeIn);

}  // namespace opossum
