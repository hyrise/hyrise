#include "table_generator.hpp"

#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "storage/chunk.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

#include "types.hpp"

#include <boost/math/distributions/skew_normal.hpp>

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
  auto chunk = Chunk();
  std::default_random_engine engine;
  std::uniform_int_distribution<int> dist(0, _max_different_value);
  for (size_t i = 0; i < _num_rows; i++) {
    /*
     * Add vectors to chunk when full, and add chunk to table.
     * Reset vectors and chunk.
     */
    if (i % vector_size == 0 && i > 0) {
      for (size_t j = 0; j < _num_columns; j++) {
        chunk.add_column(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
        value_vectors[j] = tbb::concurrent_vector<int>(vector_size);
      }
      table->emplace_chunk(std::move(chunk));
      chunk = Chunk();
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
      chunk.add_column(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
    }
    table->emplace_chunk(std::move(chunk));
  }

  if (compress) {
    DictionaryCompression::compress_table(*table);
  }

  return table;
}

// Idea: Provide an interface where we can define
// 1. How many columns we want
// 2. How many rows we want
// 3. Which columns have skew (none skew columns are sampled from std::uniform_int_distribution)
// 4. The level of skew (location, scale, shape)
// 5. How many different values per column
// TODO Maybe do it with a struct that encapsulates all required information?
std::shared_ptr<Table> TableGenerator::generate_skewed_table(
    const ChunkID chunk_size, const bool compress) {

  const size_t _skew_num_columns = 10;
  const size_t _skew_num_rows = 5 * 1000;
  const int _max_different_values = 1000;
  const double _skew_location = 0;
  const double _skew_scale = 1;
  const double _skew_shape = 0;

  std::shared_ptr<Table> table = std::make_shared<Table>(chunk_size);
  std::vector<tbb::concurrent_vector<int>> value_vectors;
  auto vector_size = chunk_size > 0 ? chunk_size : _skew_num_columns;

  /*
   * Create tables
   */
  for (size_t i = 0; i < _skew_num_rows; i++) {
    auto column_name = std::string(1, static_cast<char>(static_cast<int>('a') + i));
    table->add_column_definition(column_name, DataType::Int);
    value_vectors.emplace_back(tbb::concurrent_vector<int>(vector_size));
  }

  /*
   * Create chunks
   */
  auto chunk = Chunk();
  std::random_device rd;
  std::default_random_engine noise_generator;
  std::uniform_real_distribution<double> uniform_dist(0, 1.0);
  for (size_t i = 0; i < _skew_num_rows; i++) {
    /*
     * Add vectors to chunk when full, and add chunk to table.
     * Reset vectors and chunk.
     */
    if (i % vector_size == 0 && i > 0) {
      for (size_t j = 0; j < _skew_num_rows; j++) {
        chunk.add_column(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
        value_vectors[j] = tbb::concurrent_vector<int>(vector_size);
      }
      table->emplace_chunk(std::move(chunk));
      chunk = Chunk();
    }
    /*
     * Set random value for every column.
     */
    noise_generator.seed(rd());
    auto probability = uniform_dist(noise_generator);
    auto skew_dist = boost::math::skew_normal_distribution<double>(_skew_location, _skew_scale, _skew_shape);
    for (size_t j = 0; j < _num_columns; j++) {
      auto value = std::round(boost::math::quantile(skew_dist, probability) * _max_different_values)
      value_vectors[j][i % vector_size] = value;
    }
  }
  /*
   * Add remaining values to table, if any.
   */
  if (value_vectors[0].size() > 0) {
    for (size_t j = 0; j < _num_columns; j++) {
      chunk.add_column(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
    }
    table->emplace_chunk(std::move(chunk));
  }

  if (compress) {
    DictionaryCompression::compress_table(*table);
  }

  return table
}
}  // namespace opossum
