#pragma once

#include <time.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "random_generator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace tpcc {

class TableGenerator {
  // following TPC-C v5.11.0
 public:
  TableGenerator();

  virtual ~TableGenerator() = default;

  std::shared_ptr<opossum::Table> generate_items_table();

  std::shared_ptr<opossum::Table> generate_warehouse_table();

  std::shared_ptr<opossum::Table> generate_stock_table();

  std::shared_ptr<opossum::Table> generate_district_table();

  std::shared_ptr<opossum::Table> generate_customer_table();

  std::shared_ptr<opossum::Table> generate_history_table();

  typedef std::vector<std::vector<std::vector<size_t>>> order_line_counts_type;

  order_line_counts_type generate_order_line_counts();

  std::shared_ptr<opossum::Table> generate_order_table(order_line_counts_type order_line_counts);

  std::shared_ptr<opossum::Table> generate_order_line_table(order_line_counts_type order_line_counts);

  std::shared_ptr<opossum::Table> generate_new_order_table();

  std::shared_ptr<std::map<std::string, std::shared_ptr<opossum::Table>>> generate_all_tables();

  const size_t _chunk_size = 10000;
  const time_t _current_date = std::time(0);
  const size_t _warehouse_size = 1;

 protected:
  template <typename T>
  tbb::concurrent_vector<T> generate_order_line_column(std::vector<size_t> indices,
                                                       order_line_counts_type order_line_counts,
                                                       const std::function<T(std::vector<size_t>)> &generator_function);

  template <typename T>
  void add_column(std::shared_ptr<std::vector<size_t>> cardinalities, std::shared_ptr<opossum::Table> table,
                  std::string name, const std::function<T(std::vector<size_t>)> &generator_function);

  template <typename T>
  void add_column(std::shared_ptr<std::vector<size_t>> cardinalities, order_line_counts_type order_line_counts,
                  std::shared_ptr<opossum::Table> table, std::string name,
                  const std::function<T(std::vector<size_t>)> &generator_function);

  RandomGenerator _random_gen;
};
}  // namespace tpcc
