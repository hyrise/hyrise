#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"
#include "tpcc_base_fixture.cpp"

namespace opossum {

BENCHMARK_DEFINE_F(TPCCBenchmarkFixture, BM_getNetOrder)(benchmark::State& state) {
  clear_cache();
  // Projection::ProjectionDefinitions definitions{Projection::ProjectionDefinition{"$NO_O_ID", "float", "NO_O_ID"}};
  // auto warm_up = std::make_shared<TableScan>(_gt_new_order, ColumnName("NO_O_ID"), ">", -1);
  // warm_up->execute();
  // auto t_context = TransactionManager::get().new_transaction_context();
  // auto _gt_item = std::make_shared<GetTable>("ITEM");
  // auto _gt_warehouse = std::make_shared<GetTable>("WAREHOUSE");
  // auto _gt_stock = std::make_shared<GetTable>("STOCK");
  // auto _gt_district = std::make_shared<GetTable>("DISTRICT");
  // auto _gt_customer = std::make_shared<GetTable>("CUSTOMER");
  // auto _gt_order = std::make_shared<GetTable>("ORDER");
  // auto _gt_order_line = std::make_shared<GetTable>("ORDER");
  // _gt_item->execute();
  // _gt_warehouse->execute();
  // _gt_stock->execute();
  // _gt_district->execute();
  // _gt_customer->execute();
  // _gt_order->execute();
  // _gt_order_line->execute();

  auto _gt_new_order = std::make_shared<GetTable>("NEW-ORDER");
  _gt_new_order->execute();
  while (state.KeepRunning()) {
    for (size_t w_id = 0; w_id < _warehouse_size; ++w_id) {
      for (size_t d_id = 0; d_id < _district_size; ++d_id) {
        // SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1
        auto ts1 = std::make_shared<TableScan>(_gt_new_order, ColumnName("NO_D_ID"), "=", d_id);
        ts1->execute();
        auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("NO_W_ID"), "=", w_id);
        ts2->execute();
        auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("NO_O_ID"), ">", -1);
        ts3->execute();
        // auto limit = std::make_shared<TableScan>(ts3, 1);
        // limit->execute();
        auto proj = std::make_shared<Projection>(ts3, definitions);
        proj->execute();
        int no_o_id proj->get_output()->get_chunk(0u).get_column(0)[0];

        auto ts4 = std::make_shared<TableScan>(_gt_new_order, ColumnName("NO_O_ID"), "=", no_o_id);
        ts4->execute();
        auto delete_op = std::make_shared<Delete>("NEW-ORDER", ts4);
        delete_op->set_transaction_context(transaction_context);
        delete_op->execute();
      }
    }
  }
}

}  // namespace opossum
