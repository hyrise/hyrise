#include "table_generator.hpp"
#include <assert.h>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "types.hpp"

#include "storage/value_column.hpp"
#include "tbb/concurrent_vector.h"

namespace opossum {

std::shared_ptr<Table> TableGenerator::get_table() {
  std::shared_ptr<Table> table = std::make_shared<Table>(_chunk_size);
  std::vector<tbb::concurrent_vector<int>> value_vectors;
  auto vector_size = _chunk_size > 0 ? _chunk_size : _num_rows;
  /*
   * Generate table layout with column names from 'a' to 'z'.
   * Create a vector for each column.
   */
  for (size_t i = 0; i < _num_columns; i++) {
    auto column_name = std::string(1, static_cast<char>(static_cast<int>('a') + i));
    table->add_column(column_name, "int", false);
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
      table->add_chunk(std::move(chunk));
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
    table->add_chunk(std::move(chunk));
  }
  return table;
}
}  // namespace opossum
