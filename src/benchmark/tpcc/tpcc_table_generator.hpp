#pragma once

#include <time.h>
#include <memory>
#include <string>
#include <vector>
#include "random_generator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class TPCCTableGenerator {
 public:
  TPCCTableGenerator();

  virtual ~TPCCTableGenerator() = default;

  std::shared_ptr<Table> generate_items_table();

  std::shared_ptr<Table> generate_warehouse_table();

  std::shared_ptr<Table> generate_stock_table();

  std::shared_ptr<Table> generate_district_table();

  std::shared_ptr<Table> generate_customer_table();

  std::shared_ptr<Table> generate_history_table();

  typedef std::vector<std::vector<std::vector<size_t>>> order_line_counts_type;

  order_line_counts_type generate_order_line_counts();

  std::shared_ptr<Table> generate_order_table(order_line_counts_type order_line_counts);

  std::shared_ptr<Table> generate_order_line_table(order_line_counts_type order_line_counts);

  std::shared_ptr<Table> generate_new_order_table();

  void add_all_tables(StorageManager &manager);

 protected:
  template <typename T>
  std::shared_ptr<ValueColumn<T>> add_column(size_t cardinality, const std::function<T(size_t)> &generator_function);

  const size_t _chunk_size = 1000;
  const time_t _current_date = std::time(0);

  const size_t _item_size = 100000;
  const size_t _warehouse_size = 1;
  const size_t _stock_size = 100000;   // per warehouse
  const size_t _district_size = 10;    // per warehouse
  const size_t _customer_size = 3000;  // per district
  const size_t _history_size = 1;      // per customer
  const size_t _order_size = 3000;     // per district
  const size_t _new_order_size = 900;  // per district

  RandomGenerator _random_gen;
};
}  // namespace opossum
