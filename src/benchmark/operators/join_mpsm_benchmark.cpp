#include <memory>

#include "benchmark/benchmark.h"

#include "../benchmark_basic_fixture.hpp"
#include "../table_generator.hpp"

#include "operators/table_scan.hpp"
#include "operators/join_mpsm.hpp"

namespace opossum {

// TODO: We can override this to add own setup logic
class OperatorsProjectionBenchmark : public BenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    _chunk_size = static_cast<ChunkID>(state.range(0));

    auto table_generator = std::make_shared<TableGenerator>();

    auto table_generator2 = std::make_shared<TableGenerator>();

    _table_wrapper_a = std::make_shared<TableWrapper>(table_generator->generate_skewed_table(_chunk_size));
    _table_wrapper_b = std::make_shared<TableWrapper>(table_generator2->generate_skewed_table(_chunk_size));
    _table_dict_wrapper = std::make_shared<TableWrapper>(table_generator->generate_skewed_table(_chunk_size, true));
    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
    _table_dict_wrapper->execute();
  }
};

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_JoinMPSM)(benchmark::State& state) {
  clear_cache();
  auto scan_left = std::make_shared<TableScan>(_table_wrapper_c, ColumnID(0), ScanType::OpGreaterThanEquals, 1000); // TODO: Choose right_parameter (currently 1000)
  auto scan_right = std::make_shared<TableScan>(_table_wrapper_d, ColumnID(0), ScanType::OpGreaterThanEquals, 1000); // TODO: right_parameter
  scan_left->execute();
  scan_right->execute();

  while (state.KeepRunning()) {
  	// TODO Choose our join
    // auto join = std::make_shared<JoinHash>(scan_left, scan_right, JoinMode::Inner,
    //                                     std::pair<ColumnID, ColumnID>(ColumnID(0), ColumnID(0)), ScanType::OpEquals);
    // join->execute() ;
  }
}

}  // namespace opossum