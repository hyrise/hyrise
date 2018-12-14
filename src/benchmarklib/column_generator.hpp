#pragma once

#include <memory>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "table_generator.hpp"

namespace opossum {

class Table;

class ColumnGenerator {

 public:
  ColumnGenerator();

  template<typename VectorType>
  VectorType generate_value_vector(const ColumnDataDistribution& column_data_distribution, size_t row_count,
    const std::function<bool(size_t)>& allow_value);
  std::vector<size_t> generate_join_partner(const std::vector<size_t>& unique_values, size_t row_count,
    float selectivity, const std::function<size_t(double)>& get_value_with_no_join_partner);

  std::unique_ptr<std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>>>
    generate_join_pair(std::vector<double>& selectivities, size_t chunk_size, size_t row_count_table1,
        size_t row_count_table2);

 protected:
  // using mt19937 because std::default_random engine is not guaranteed to be a sensible default
  std::mt19937 _pseudorandom_engine;
  std::uniform_real_distribution<> _probability_dist;

};

} // namespace opposum

