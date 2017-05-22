#include <memory>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/limit.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/scheduler/operator_task.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

class TPCCPaymentBenchmark : public TPCCBenchmarkFixture {
 public:
};

BENCHMARK_F(TPCCPaymentBenchmark, BM_TPCC_Payment)(benchmark::State &state) {
  clear_cache();

  //    auto home_warehouse_id = 0;  // there is only one warehouse

  while (state.KeepRunning()) {
    //    // pass in i>1000 to trigger random value generation
    //    auto c_last = _random_gen.last_name(2000);
    //    auto d_id = _random_gen.number(1, 10);
    //
    //    auto c_id = _random_gen.nurand(1023, 1, 3000);
    //
    //    auto use_last_name = _random_gen.number(0, 100) < 60;
    //    auto use_home_warehouse = _random_gen.number(0, 100) < 85;
  }
}

}  // namespace opossum
