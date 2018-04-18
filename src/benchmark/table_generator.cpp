#include "table_generator.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "boost/math/distributions/pareto.hpp"
#include "boost/math/distributions/skew_normal.hpp"
#include "boost/math/distributions/uniform.hpp"

#include "tbb/concurrent_vector.h"

#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

#include "types.hpp"

namespace opossum {

std::shared_ptr<Table> TableGenerator::generate_table(const ChunkID chunk_size,
                                                      std::optional<EncodingType> encoding_type) {
  std::vector<tbb::concurrent_vector<int>> value_vectors;
  auto vector_size = std::min(static_cast<size_t>(chunk_size), _num_rows);
  /*
   * Generate table layout with enumerated column names (i.e., "col_1", "col_2", ...)
   * Create a vector for each column.
   */
  TableColumnDefinitions column_definitions;
  for (size_t i = 0; i < _num_columns; i++) {
    auto column_name = std::string(1, static_cast<char>(static_cast<int>('a') + i));
    column_definitions.emplace_back(column_name, DataType::Int);
    value_vectors.emplace_back(tbb::concurrent_vector<int>(vector_size));
  }
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size);
  std::default_random_engine engine;
  std::uniform_int_distribution<int> dist(0, _max_different_value);
  for (size_t i = 0; i < _num_rows; i++) {
    /*
     * Add vectors to chunk when full, and add chunk to table.
     * Reset vectors and chunk.
     */
    if (i % vector_size == 0 && i > 0) {
      ChunkColumns columns;
      for (size_t j = 0; j < _num_columns; j++) {
        columns.push_back(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
        value_vectors[j] = tbb::concurrent_vector<int>(vector_size);
      }
      table->append_chunk(columns);
    }
    /*
     * Set random value for every column.
     */
    for (size_t j = 0; j < _num_columns; j++) {
      value_vectors[j][i % vector_size] = dist(engine);
    }
  }
  /*
   * Add remaining values to table, if any.
   */
  if (value_vectors[0].size() > 0) {
    ChunkColumns columns;
    for (size_t j = 0; j < _num_columns; j++) {
      columns.push_back(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
    }
    table->append_chunk(columns);
  }

  if (encoding_type.has_value()) {
    ChunkEncoder::encode_all_chunks(table, {encoding_type.value()});
  }

  return table;
}

std::shared_ptr<Table> TableGenerator::generate_table(
    const std::vector<ColumnDataDistribution>& column_data_distributions, const size_t num_rows,
    const size_t chunk_size, std::optional<EncodingType> encoding_type) {
  const auto num_columns = column_data_distributions.size();
  const auto num_chunks = std::ceil(static_cast<double>(num_rows) / static_cast<double>(chunk_size));

  // create result table and container for vectors holding the generated values for the columns
  std::vector<tbb::concurrent_vector<int>> value_vectors;

  // add column definitions and initialize each value vector
  TableColumnDefinitions column_definitions;
  for (size_t column = 1; column <= num_columns; ++column) {
    auto column_name = "col_" + std::to_string(column);
    column_definitions.emplace_back(column_name, DataType::Int);
    value_vectors.emplace_back(tbb::concurrent_vector<int>(chunk_size));
  }
  std::shared_ptr<Table> table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size);

  std::random_device rd;
  // using mt19937 because std::default_random engine is not guaranteed to be a sensible default
  auto pseudorandom_engine = std::mt19937{};

  auto probability_dist = std::uniform_real_distribution{0.0, 1.0};
  auto generate_value_by_distribution_type = std::function<int(void)>{};

  pseudorandom_engine.seed(rd());

  for (ChunkID chunk_index{0}; chunk_index < num_chunks; ++chunk_index) {
    ChunkColumns columns;
    for (ChunkID column_index{0}; column_index < num_columns; ++column_index) {
      const auto& column_data_distribution = column_data_distributions[column_index];

      // generate distribution from column configuration
      switch (column_data_distribution.distribution_type) {
        case DataDistributionType::Uniform: {
          auto uniform_dist = boost::math::uniform_distribution<double>{column_data_distribution.min_value,
                                                                        column_data_distribution.max_value};
          generate_value_by_distribution_type = [uniform_dist, &probability_dist, &pseudorandom_engine]() {
            const auto probability = probability_dist(pseudorandom_engine);
            return static_cast<int>(std::floor(boost::math::quantile(uniform_dist, probability)));
          };
          break;
        }
        case DataDistributionType::NormalSkewed: {
          auto skew_dist = boost::math::skew_normal_distribution<double>{column_data_distribution.skew_location,
                                                                         column_data_distribution.skew_scale,
                                                                         column_data_distribution.skew_shape};
          generate_value_by_distribution_type = [skew_dist, &probability_dist, &pseudorandom_engine]() {
            const auto probability = probability_dist(pseudorandom_engine);
            return static_cast<int>(std::round(boost::math::quantile(skew_dist, probability) * 10));
          };
          break;
        }
        case DataDistributionType::Pareto: {
          auto pareto_dist = boost::math::pareto_distribution<double>{column_data_distribution.pareto_scale,
                                                                      column_data_distribution.pareto_shape};
          generate_value_by_distribution_type = [pareto_dist, &probability_dist, &pseudorandom_engine]() {
            const auto probability = probability_dist(pseudorandom_engine);
            return static_cast<int>(std::floor(boost::math::quantile(pareto_dist, probability)));
          };
          break;
        }
      }

      // generate values according to distribution
      for (size_t row_offset{0}; row_offset < chunk_size; ++row_offset) {
        // bounds check
        if ((chunk_index + 1) * chunk_size + (row_offset + 1) > num_rows) {
          break;
        }

        value_vectors[column_index][row_offset] = generate_value_by_distribution_type();
      }

      // add values to column in chunk, reset value vector
      columns.push_back(std::make_shared<ValueColumn<int>>(std::move(value_vectors[column_index])));
      value_vectors[column_index] = tbb::concurrent_vector<int>(chunk_size);

      // add full chunk to table
      if (column_index == num_columns - 1) {
        table->append_chunk(columns);
      }
    }
  }

  if (encoding_type.has_value()) {
    ChunkEncoder::encode_all_chunks(table, ColumnEncodingSpec{encoding_type.value()});
  }

  return table;
}
}  // namespace opossum
