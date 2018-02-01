#include "table_generator.hpp"

#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include <boost/math/distributions/pareto.hpp>
#include <boost/math/distributions/skew_normal.hpp>
#include <boost/math/distributions/uniform.hpp>

#include "storage/chunk.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

#include "types.hpp"

namespace opossum {

std::shared_ptr<Table> TableGenerator::generate_table(const ChunkID chunk_size, const bool compress) {
  std::shared_ptr<Table> table = std::make_shared<Table>(chunk_size);
  std::vector<tbb::concurrent_vector<int>> value_vectors;
  auto vector_size = chunk_size > 0 ? chunk_size : _num_rows;
  /*
   * Generate table layout with column names from 'a' to 'z'.
   * Create a vector for each column.
   */
  for (size_t i = 0; i < _num_columns; i++) {
    auto column_name = std::string(1, static_cast<char>(static_cast<int>('a') + i));
    table->add_column_definition(column_name, DataType::Int);
    value_vectors.emplace_back(tbb::concurrent_vector<int>(vector_size));
  }
  auto chunk = std::make_shared<Chunk>();
  std::default_random_engine engine;
  std::uniform_int_distribution<int> dist(0, _max_different_value);
  for (size_t i = 0; i < _num_rows; i++) {
    /*
     * Add vectors to chunk when full, and add chunk to table.
     * Reset vectors and chunk.
     */
    if (i % vector_size == 0 && i > 0) {
      for (size_t j = 0; j < _num_columns; j++) {
        chunk->add_column(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
        value_vectors[j] = tbb::concurrent_vector<int>(vector_size);
      }
      table->emplace_chunk(std::move(chunk));
      chunk = std::make_shared<Chunk>();
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
    for (size_t j = 0; j < _num_columns; j++) {
      chunk->add_column(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
    }
    table->emplace_chunk(std::move(chunk));
  }

  if (compress) {
    DictionaryCompression::compress_table(*table);
  }

  return table;
}

std::shared_ptr<Table> TableGenerator::generate_table(const std::vector<ColumnConfiguration>& column_configurations,
                                                      const bool compress, const size_t num_rows,
                                                      const ChunkID chunk_size) {
  const auto num_columns = column_configurations.size();
  const auto num_chunks = std::ceil(static_cast<double>(num_rows) / static_cast<double>(chunk_size));

  // create result table and container for vectors holding the generated values for the columns
  std::shared_ptr<Table> table = std::make_shared<Table>(chunk_size);
  std::vector<tbb::concurrent_vector<int>> value_vectors;

  // add column definitions and initialize each value vector
  for (size_t column = 0; column < num_columns; ++column) {
    auto column_name = std::string(1, static_cast<char>(static_cast<int>('a') + column));
    table->add_column_definition(column_name, DataType::Int);
    value_vectors.emplace_back(tbb::concurrent_vector<int>(chunk_size));
  }

  auto chunk = Chunk();

  std::random_device rd;
  auto noise_generator = std::default_random_engine{};

  auto uniform_dist = boost::math::uniform_distribution<double>{0.0, 1.0};
  auto skew_dist = boost::math::skew_normal_distribution<double>(0.0, 1.0, 0.0);
  auto pareto_dist = boost::math::pareto_distribution<double>{1.0, 1.0};

  noise_generator.seed(rd());

  for (auto chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
    for (auto column_index = 0; column_index < num_columns; ++column_index) {
      const auto& column_configuration = column_configurations[column_index];

      // generate distribution from column configuration
      switch (column_configuration.distribution_type) {
        case uniform:
          uniform_dist =
              boost::math::uniform_distribution<double>{column_configuration.min_value, column_configuration.max_value};
          break;
        case normal_skewed:
          skew_dist = boost::math::skew_normal_distribution<double>{
              column_configuration.skew_location, column_configuration.skew_scale, column_configuration.skew_shape};
          break;
        case pareto:
          pareto_dist = boost::math::pareto_distribution<double>{column_configuration.pareto_scale,
                                                                 column_configuration.pareto_shape};
          break;
      }

      // generate values according to distribution
      for (auto row_index = 0; row_index < chunk_size; ++row_index) {
        // bounds check
        if ((chunk_index + 1) * chunk_size + (row_index + 1) > num_rows) {
          break;
        }

        const auto probability = uniform_dist(noise_generator);
        int value;
        switch (column_configuration.distribution_type) {
          case uniform:
            value = std::floor(boost::math::quantile(uniform_dist, probability));
            break;
          case normal_skewed:
            value = std::round(boost::math::quantile(skew_dist, probability) * 10);
            break;
          case pareto:
            value = std::floor(boost::math::quantile(pareto_dist, probability));
            break;
        }
        value_vectors[column_index][row_index] = value;
      }

      // add values to column in chunk, reset value vector
      chunk.add_column(std::make_shared<ValueColumn<int>>(std::move(value_vectors[column_index])));
      value_vectors[column_index] = tbb::concurrent_vector<int>(chunk_size);

      // add full chunk to table
      if (column_index == num_columns - 1) {
        table->emplace_chunk(std::move(chunk));
        chunk = Chunk();
      }
    }
  }

  if (compress) {
    DictionaryCompression::compress_table(*table);
  }

  return table;
}
}  // namespace opossum
