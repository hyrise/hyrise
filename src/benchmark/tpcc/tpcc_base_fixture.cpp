#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"
#include "tpcc_table_generator.hpp"

namespace opossum {

// Defining the base fixture class
class TPCCBenchmarkFixture : public benchmark::Fixture {
 public:
  TPCCBenchmarkFixture() {
    // Generating TPCC tables

    TPCCTableGenerator generator;
    generator.add_all_tables(opossum::StorageManager::get());

    _gt_item = std::make_shared<GetTable>("ITEM");
    _gt_warehouse = std::make_shared<GetTable>("WAREHOUSE");
    _gt_stock = std::make_shared<GetTable>("STOCK");
    _gt_district = std::make_shared<GetTable>("DISTRICT");
    _gt_customer = std::make_shared<GetTable>("CUSTOMER");
    _gt_history = std::make_shared<GetTable>("HISTORY");
    _gt_order = std::make_shared<GetTable>("ORDER");
    _gt_order_line = std::make_shared<GetTable>("ORDER");
    _gt_new_order = std::make_shared<GetTable>("NEW-ORDER");
    _gt_item->execute();
    _gt_warehouse->execute();
    _gt_stock->execute();
    _gt_district->execute();
    _gt_customer->execute();
    _gt_history->execute();
    _gt_order->execute();
    _gt_order_line->execute();
    _gt_new_order->execute();
  }

  virtual void TearDown(const ::benchmark::State&) { opossum::StorageManager::get().reset(); }

 protected:
  std::shared_ptr<GetTable> _gt_item;
  std::shared_ptr<GetTable> _gt_warehouse;
  std::shared_ptr<GetTable> _gt_stock;
  std::shared_ptr<GetTable> _gt_district;
  std::shared_ptr<GetTable> _gt_customer;
  std::shared_ptr<GetTable> _gt_history;
  std::shared_ptr<GetTable> _gt_order;
  std::shared_ptr<GetTable> _gt_order_line;
  std::shared_ptr<GetTable> _gt_new_order;

  void clear_cache() {
    std::vector<int> clear = std::vector<int>();
    clear.resize(500 * 1000 * 1000, 42);
    for (uint i = 0; i < clear.size(); i++) {
      clear[i] += 1;
    }
    clear.resize(0);
  }
};
}  // namespace opossum
