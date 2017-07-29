#pragma once

#include <time.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "benchmark_utilities/abstract_benchmark_table_generator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "tpcc_random_generator.hpp"

namespace tpcc {

class TableGenerator : public benchmark_utilities::AbstractBenchmarkTableGenerator {
  // following TPC-C v5.11.0
 public:
  explicit TableGenerator(const size_t chunk_size = 10000, const size_t warehouse_size = 1);

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

  const size_t _chunk_size;
  const size_t _warehouse_size;
  const time_t _current_date = std::time(0);

 protected:
  template <typename T>
  std::vector<T> generate_inner_order_line_column(std::vector<size_t> indices, order_line_counts_type order_line_counts,
                                                  const std::function<T(std::vector<size_t>)> &generator_function);

  template <typename T>
  void add_order_line_column(std::shared_ptr<opossum::Table> table, std::string name,
                             std::shared_ptr<std::vector<size_t>> cardinalities,
                             TableGenerator::order_line_counts_type order_line_counts,
                             const std::function<T(std::vector<size_t>)> &generator_function);

  TpccRandomGenerator _random_gen;
};
}  // namespace tpcc
