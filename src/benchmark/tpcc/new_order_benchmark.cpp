#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/scheduler/operator_task.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

class TPCCNewOrderBenchmark : public TPCCBenchmarkFixture {
 public:
  std::vector<std::shared_ptr<OperatorTask>> get_district_tasks(unsigned int d_id, unsigned int d_w_id) {
    /**
     * SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?
     */

    // Operators
    auto gt = std::make_shared<GetTable>("DISTRICT");

    auto ts1 = std::make_shared<TableScan>(gt, "D_ID", "=", 1);
    auto ts2 = std::make_shared<TableScan>(ts1, "D_W_ID", "=", 1);

    std::vector<std::string> columns = {"D_TAX", "D_NEXT_O_ID"};
    auto proj = std::make_shared<Projection>(ts2, columns);

    // Tasks
    auto gt_t = std::make_shared<OperatorTask>(gt);
    auto ts1_t = std::make_shared<OperatorTask>(ts1);
    auto ts2_t = std::make_shared<OperatorTask>(ts2);
    auto proj_t = std::make_shared<OperatorTask>(proj);

    // Dependencies
    gt_t->set_as_predecessor_of(ts1_t);
    ts1_t->set_as_predecessor_of(ts2_t);
    ts2_t->set_as_predecessor_of(proj_t);

    std::vector<std::shared_ptr<OperatorTask>> tasks = {gt_t, ts1_t, ts2_t, proj_t};
    return tasks;
  }
};

BENCHMARK_F(TPCCNewOrderBenchmark, BM_TPCC_NewOrder)(benchmark::State& state) {
  clear_cache();

  auto d_id = 1;
  auto d_w_id = 1;

  while (state.KeepRunning()) {
    auto tasks = get_district_tasks(d_id, d_w_id);
    for (auto& task : tasks) {
      task->schedule();
    }

    auto last_task = tasks.back();
    last_task->join();

    auto output = last_task->get_operator()->get_output();
  }
}

}  // namespace opossum
