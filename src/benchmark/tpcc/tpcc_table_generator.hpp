#pragma once

#include <memory>
#include <string>
#include "random_generator.hpp"
#include "storage/table.hpp"

namespace opossum {

class TPCCTableGenerator {
 public:
  TPCCTableGenerator();

  virtual ~TPCCTableGenerator() = default;

  template <typename T>
  std::shared_ptr<ValueColumn<T>> add_column(size_t cardinality, const std::function<T(size_t)> &generator_function);

  std::shared_ptr<Table> generate_items_table();

  std::shared_ptr<Table> generate_warehouse_table();

  std::shared_ptr<Table> generate_stock_table();

  std::shared_ptr<Table> generate_district_table();

  std::shared_ptr<Table> generate_customer_table();

 protected:
  const size_t _chunk_size = 1000;

  const size_t _item_size = 100000;
  const size_t _warehouse_size = 1;
  const size_t _stock_size = 100000;
  const size_t _district_size = 10;
  const size_t _customer_size = 3000;

  RandomGenerator _random_gen;
};
}  // namespace opossum
