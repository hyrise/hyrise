#pragma once

#include <ctime>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "benchmark_utilities/abstract_benchmark_table_generator.hpp"
#include "benchmark_utils.hpp"
#include "tpcc_random_generator.hpp"

namespace opossum {

class Table;

using TpccTableGeneratorFunctions = std::unordered_map<std::string, std::function<std::shared_ptr<Table>()>>;

class TpccTableGenerator : public opossum::AbstractBenchmarkTableGenerator {
  // following TPC-C v5.11.0
 public:
  explicit TpccTableGenerator(const ChunkOffset chunk_size = 1'000'000, const size_t warehouse_size = 1,
                              EncodingConfig encoding_config = EncodingConfig{});

  virtual ~TpccTableGenerator() = default;

  std::shared_ptr<Table> generate_items_table();

  std::shared_ptr<Table> generate_warehouse_table();

  std::shared_ptr<Table> generate_stock_table();

  std::shared_ptr<Table> generate_district_table();

  std::shared_ptr<Table> generate_customer_table();

  std::shared_ptr<Table> generate_history_table();

  typedef std::vector<std::vector<std::vector<size_t>>> order_line_counts_type;

  order_line_counts_type generate_order_line_counts();

  std::shared_ptr<Table> generate_order_table(const order_line_counts_type& order_line_counts);

  std::shared_ptr<Table> generate_order_line_table(const order_line_counts_type& order_line_counts);

  std::shared_ptr<Table> generate_new_order_table();

  std::map<std::string, std::shared_ptr<Table>> generate_all_tables() override;

  static TpccTableGeneratorFunctions table_generator_functions();

  std::shared_ptr<Table> generate_table(const std::string& table_name);

  const size_t _warehouse_size;
  const time_t _current_date = std::time(0);
  const EncodingConfig _encoding_config;

 protected:
  void _encode_table(const std::string& table_name, const std::shared_ptr<Table>& table);

  template <typename T>
  std::vector<T> _generate_inner_order_line_column(std::vector<size_t> indices,
                                                   order_line_counts_type order_line_counts,
                                                   const std::function<T(std::vector<size_t>)>& generator_function);

  template <typename T>
  void _add_order_line_column(std::vector<ChunkColumns>& columns_by_chunk, TableColumnDefinitions& column_definitions,
                              std::string name, std::shared_ptr<std::vector<size_t>> cardinalities,
                              order_line_counts_type order_line_counts,
                              const std::function<T(std::vector<size_t>)>& generator_function);

  TpccRandomGenerator _random_gen;
};
}  // namespace opossum
