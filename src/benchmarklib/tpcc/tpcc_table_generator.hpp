#pragma once

#include <ctime>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "benchmark_utilities/abstract_benchmark_table_generator.hpp"
#include "tpcc_random_generator.hpp"

namespace opossum {

class Table;

}  // namespace opossum

namespace tpcc {

using TpccTableGeneratorFunctions = std::unordered_map<std::string, std::function<opossum::TableSPtr()>>;

class TpccTableGenerator : public benchmark_utilities::AbstractBenchmarkTableGenerator {
  // following TPC-C v5.11.0
 public:
  explicit TpccTableGenerator(const opossum::ChunkOffset chunk_size = 1'000'000, const size_t warehouse_size = 1);

  virtual ~TpccTableGenerator() = default;

  opossum::TableSPtr generate_items_table();

  opossum::TableSPtr generate_warehouse_table();

  opossum::TableSPtr generate_stock_table();

  opossum::TableSPtr generate_district_table();

  opossum::TableSPtr generate_customer_table();

  opossum::TableSPtr generate_history_table();

  typedef std::vector<std::vector<std::vector<size_t>>> order_line_counts_type;

  order_line_counts_type generate_order_line_counts();

  opossum::TableSPtr generate_order_table(order_line_counts_type order_line_counts);

  opossum::TableSPtr generate_order_line_table(order_line_counts_type order_line_counts);

  opossum::TableSPtr generate_new_order_table();

  std::map<std::string, opossum::TableSPtr> generate_all_tables();

  static TpccTableGeneratorFunctions tpcc_table_generator_functions();

  static opossum::TableSPtr generate_tpcc_table(const std::string& tablename);

  const size_t _warehouse_size;
  const time_t _current_date = std::time(0);

 protected:
  template <typename T>
  std::vector<T> generate_inner_order_line_column(std::vector<size_t> indices, order_line_counts_type order_line_counts,
                                                  const std::function<T(std::vector<size_t>)>& generator_function);

  template <typename T>
  void add_order_line_column(std::vector<opossum::ChunkColumns>& columns_by_chunk,
                             opossum::TableColumnDefinitions& column_definitions, std::string name,
                             std::shared_ptr<std::vector<size_t>> cardinalities,
                             order_line_counts_type order_line_counts,
                             const std::function<T(std::vector<size_t>)>& generator_function);

  TpccRandomGenerator _random_gen;
};
}  // namespace tpcc
