#include <memory>

#include "benchmark/benchmark.h"

#include "../benchmark_basic_fixture.hpp"
#include "../table_generator.hpp"

#include "operators/table_scan.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_JoinHash)(benchmark::State& state) {
  clear_cache();
  auto scan_left = std::make_shared<TableScan>(_table_wrapper_c, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  auto scan_right = std::make_shared<TableScan>(_table_wrapper_d, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  scan_left->execute();
  scan_right->execute();

  while (state.KeepRunning()) {
    auto join = std::make_shared<JoinHash>(scan_left, scan_right, JoinMode::Inner,
                                        std::pair<ColumnID, ColumnID>(ColumnID(0), ColumnID(0)), ScanType::OpEquals);
    join->execute() ;
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_JoinSortMerge1Partition)(benchmark::State& state) {
  clear_cache();
  auto scan_left = std::make_shared<TableScan>(_table_wrapper_c, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  auto scan_right = std::make_shared<TableScan>(_table_wrapper_d, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  scan_left->execute();
  scan_right->execute();

  while (state.KeepRunning()) {
    auto join = std::make_shared<JoinSortMerge>(scan_left, scan_right, JoinMode::Inner,
                                      std::pair<ColumnID, ColumnID>(ColumnID(0), ColumnID(0)), ScanType::OpEquals, 1);
    join->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_JoinSortMerge2Partitions)(benchmark::State& state) {
  clear_cache();
  auto scan_left = std::make_shared<TableScan>(_table_wrapper_c, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  auto scan_right = std::make_shared<TableScan>(_table_wrapper_d, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  scan_left->execute();
  scan_right->execute();

  while (state.KeepRunning()) {
    auto join = std::make_shared<JoinSortMerge>(scan_left, scan_right, JoinMode::Inner,
                                      std::pair<ColumnID, ColumnID>(ColumnID(0), ColumnID(0)), ScanType::OpEquals, 2);
    join->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_JoinSortMerge4Partitions)(benchmark::State& state) {
  clear_cache();
  auto scan_left = std::make_shared<TableScan>(_table_wrapper_c, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  auto scan_right = std::make_shared<TableScan>(_table_wrapper_d, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  scan_left->execute();
  scan_right->execute();

  while (state.KeepRunning()) {
    auto join = std::make_shared<JoinSortMerge>(scan_left, scan_right, JoinMode::Inner,
                                      std::pair<ColumnID, ColumnID>(ColumnID(0), ColumnID(0)), ScanType::OpEquals, 4);
    join->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_JoinSortMerge8Partitions)(benchmark::State& state) {
  clear_cache();
  auto scan_left = std::make_shared<TableScan>(_table_wrapper_c, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  auto scan_right = std::make_shared<TableScan>(_table_wrapper_d, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  scan_left->execute();
  scan_right->execute();

  while (state.KeepRunning()) {
    auto join = std::make_shared<JoinSortMerge>(scan_left, scan_right, JoinMode::Inner,
                                      std::pair<ColumnID, ColumnID>(ColumnID(0), ColumnID(0)), ScanType::OpEquals, 8);
    join->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_JoinSortMerge128Partitions)(benchmark::State& state) {
  clear_cache();
  auto scan_left = std::make_shared<TableScan>(_table_wrapper_c, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  auto scan_right = std::make_shared<TableScan>(_table_wrapper_d, ColumnID(0), ScanType::OpGreaterThanEquals, 1000);
  scan_left->execute();
  scan_right->execute();

  while (state.KeepRunning()) {
    auto join = std::make_shared<JoinSortMerge>(scan_left, scan_right, JoinMode::Inner,
                                    std::pair<ColumnID, ColumnID>(ColumnID(0), ColumnID(0)), ScanType::OpEquals, 128);
    join->execute();
  }
}

BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_JoinHash)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_JoinSortMerge1Partition)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_JoinSortMerge2Partitions)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_JoinSortMerge4Partitions)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_JoinSortMerge8Partitions)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_JoinSortMerge128Partitions)->Apply(BenchmarkBasicFixture::ChunkSizeIn);

}  // namespace opossum