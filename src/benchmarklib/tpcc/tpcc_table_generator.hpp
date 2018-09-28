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

class TpccTableGenerator : AbstractBenchmarkTableGenerator {
  // following TPC-C v5.11.0
 public:
  explicit TpccTableGenerator(const ChunkOffset chunk_size = 1'000'000, const size_t warehouse_size = 1,
                              EncodingConfig encoding_config = EncodingConfig{}, bool store = false);

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
  TpccRandomGenerator _random_gen;

 protected:
  void _encode_table(const std::string& table_name, const std::shared_ptr<Table>& table);

  template <typename T>
  std::vector<T> _generate_inner_order_line_column(std::vector<size_t> indices,
                                                   order_line_counts_type order_line_counts,
                                                   const std::function<T(std::vector<size_t>)>& generator_function);

  template <typename T>
  void _add_order_line_column(std::vector<Segments>& segments_by_chunk, TableColumnDefinitions& column_definitions,
                              std::string name, std::shared_ptr<std::vector<size_t>> cardinalities,
                              order_line_counts_type order_line_counts,
                              const std::function<T(std::vector<size_t>)>& generator_function);

  /**
   * In TPCC and TPCH table sizes are usually defined relatively to each other.
   * E.g. the specification defines that there are 10 districts for each warehouse.
   *
   * A trivial approach to implement this in our table generator would be to iterate in nested loops and add all rows.
   * However, this makes it hard to take care of a certain chunk_size. With nested loops
   * chunks only contain as many rows as there are iterations in the most inner loop.
   *
   * In this method we basically generate the whole column in a single loop,
   * so that we can easily split when a Chunk is full. To do that we have all the cardinalities of the influencing
   * tables:
   * E.g. for the CUSTOMER table we have the following cardinalities:
   * indices[0] = warehouse_size = 1
   * indices[1] = district_size = 10
   * indices[2] = customer_size = 3000
   * So in total we have to generate 1*10*3000 = 30 000 customers.
   *
   * @tparam T                  the type of the column
   * @param table               the column shall be added to this table as well as column metadata
   * @param name                the name of the column
   * @param cardinalities       the cardinalities of the different 'nested loops',
   *                            e.g. 10 districts per warehouse results in {1, 10}
   * @param generator_function  a lambda function to generate a vector of values for this column
   */
  template <typename T>
  void _add_column(std::vector<opossum::Segments>& segments_by_chunk,
                  opossum::TableColumnDefinitions& column_definitions, std::string name,
                  std::shared_ptr<std::vector<size_t>> cardinalities,
                  const std::function<std::vector<T>(std::vector<size_t>)>& generator_function);

  /**
   * This method simplifies the interface for columns
   * where only a single element is added in the inner loop.
   *
   * @tparam T                  the type of the column
   * @param table               the column shall be added to this table as well as column metadata
   * @param name                the name of the column
   * @param cardinalities       the cardinalities of the different 'nested loops',
   *                            e.g. 10 districts per warehouse results in {1, 10}
   * @param generator_function  a lambda function to generate a value for this column
   */
  template <typename T>
  void _add_column(std::vector<opossum::Segments>& segments_by_chunk,
                  opossum::TableColumnDefinitions& column_definitions, std::string name,
                  std::shared_ptr<std::vector<size_t>> cardinalities,
                  const std::function<T(std::vector<size_t>)>& generator_function);
};
}  // namespace opossum
