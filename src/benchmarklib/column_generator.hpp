#pragma once

#include <memory>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "storage/table_column_definition.hpp"

#include "table_generator.hpp"

namespace opossum {

class Table;

class ColumnGenerator {
 public:
  ColumnGenerator();

  template <typename VectorType>
  VectorType generate_value_vector(const ColumnDataDistribution& column_data_distribution, size_t row_count,
                                   const std::function<bool(int)>& allow_value);

  std::vector<int> generate_join_partner(const std::vector<int>& join_partner, size_t row_count, double selectivity,
                                         const std::function<int(double)>& get_value_with_no_join_partner);

  std::unique_ptr<std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>>> generate_joinable_table_pair(
      const std::vector<double>& selectivities, size_t chunk_size, size_t row_count_table1, size_t row_count_table2,
      uint32_t min_value, uint32_t max_value, const std::function<bool(int)>& allow_value,
      const std::function<int(double)>& get_value_without_join_partner);

  static std::shared_ptr<Table> create_table(const TableColumnDefinitions& column_definitions, size_t chunk_size,
                                             const std::vector<std::vector<int>>& table_data);

  std::unique_ptr<std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>>> generate_two_predicate_join_tables(
      size_t chunk_size, size_t fact_table_size, size_t fact_factor, double probing_factor);

 protected:
  // using mt19937 because std::default_random engine is not guaranteed to be a sensible default
  std::mt19937 _pseudorandom_engine;
  std::uniform_real_distribution<> _probability_dist;
};

}  // namespace opossum
