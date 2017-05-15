#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/operators/aggregate.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/scheduler/operator_task.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

BENCHMARK_F(TPCCBenchmarkFixture, BM_OrderStatus)(benchmark::State& state) {
  clear_cache();
  //  auto warm_up = std::make_shared<Difference>(_gt_a, _gt_b);
  //  warm_up->execute();
  while (state.KeepRunning()) {
    /**
     * EXEC SQL SELECT count(c_id) INTO :namecnt
     * FROM customer
     * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
     */

    auto c_last = 1;
    auto c_d_id = 1;
    auto c_w_id = 1;

    auto gt = std::make_shared<GetTable>("CUSTOMER");
    auto first_filter = std::make_shared<TableScan>(gt, "C_LAST", "=", c_last);
    auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
    auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
    // TODO(anyone): replace Sum by Count as soon as merged
    auto count = std::make_shared<Aggregate>(
        third_filter, std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(std::string("C_ID"), Sum)},
        std::vector<std::string>{});

    auto gt_task = std::make_shared<OperatorTask>(gt);
    auto first_filter_task = std::make_shared<OperatorTask>(first_filter);
    auto second_filter_task = std::make_shared<OperatorTask>(second_filter);
    auto third_filter_task = std::make_shared<OperatorTask>(third_filter);
    auto count_task = std::make_shared<OperatorTask>(count);

    gt_task->set_as_predecessor_of(first_filter_task);
    first_filter_task->set_as_predecessor_of(second_filter_task);
    second_filter_task->set_as_predecessor_of(third_filter_task);
    third_filter_task->set_as_predecessor_of(count_task);

    gt_task->schedule();
    first_filter_task->schedule();
    second_filter_task->schedule();
    third_filter_task->schedule();
    count_task->schedule();

    count_task->join();

    auto output = count_task->get_operator()->get_output();

    /**
     * EXEC SQL DECLARE c_name CURSOR FOR
     * SELECT c_balance, c_first, c_middle, c_id
     * FROM customer
     * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
     * ORDER BY c_first;
     * EXEC SQL OPEN c_name;
     */
  }
}

}  // namespace opossum
