#pragma once

#include <functional>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace benchmark_utilities {

class BenchmarkTableGenerator {
 public:
  explicit BenchmarkTableGenerator(const size_t chunk_size = 1000) : _chunk_size(chunk_size) {}

  virtual ~BenchmarkTableGenerator() = default;

  virtual std::shared_ptr<std::map<std::string, std::shared_ptr<opossum::Table>>> generate_all_tables() = 0;

 protected:
  const size_t _chunk_size;

  template <typename T>
  void add_column(std::shared_ptr<opossum::Table> table, std::string name,
                  std::shared_ptr<std::vector<size_t>> cardinalities,
                  const std::function<T(std::vector<size_t>)> &generator_function) {
    /**
     * We have to add Chunks when we add the first column.
     * This has to be made after the first column was created and added,
     * because empty Chunks would be pruned right away.
     */
    bool is_first_column = table->col_count() == 0;

    auto data_type_name = opossum::name_of_type<T>();
    table->add_column_definition(name, data_type_name);

    /**
     * Calculate the total row count for this column based on the cardinalities of the influencing tables.
     * For the CUSTOMER table this calculates 1*10*3000
     */
    auto row_count =
        std::accumulate(std::begin(*cardinalities), std::end(*cardinalities), 1u, std::multiplies<size_t>());

    tbb::concurrent_vector<T> column;
    column.reserve(_chunk_size);

    /**
     * The loop over all records that the final column of the table will contain, e.g. row_count = 30 000 for CUSTOMER
     */
    for (size_t row_index = 0; row_index < row_count; row_index++) {
      std::vector<size_t> indices(cardinalities->size());

      /**
       * Calculate indices for internal loops
       *
       * We have to take care of writing IDs for referenced table correctly, e.g. when they are used as foreign key.
       * In that case the 'generator_function' has to be able to access the current index of our loops correctly,
       * which we ensure by defining them here.
       *
       * For example for CUSTOMER:
       * WAREHOUSE_ID | DISTRICT_ID | CUSTOMER_ID
       * indices[0]   | indices[1]  | indices[2]
       */
      for (size_t loop = 0; loop < cardinalities->size(); loop++) {
        auto divisor = std::accumulate(std::begin(*cardinalities) + loop + 1, std::end(*cardinalities), 1u,
                                       std::multiplies<size_t>());
        indices[loop] = (row_index / divisor) % cardinalities->at(loop);
      }

      /**
       * Actually generating and adding values.
       * Pass in the previously generated indices to use them in 'generator_function',
       * e.g. when generating IDs.
       */
      column.push_back(generator_function(indices));

      // write output chunks if column size has reached chunk_size
      if (row_index % _chunk_size == _chunk_size - 1) {
        auto value_column = std::make_shared<opossum::ValueColumn<T>>(std::move(column));
        opossum::ChunkID chunk_id{static_cast<uint32_t>(row_index / _chunk_size)};

        // add Chunk if it is the first column, e.g. WAREHOUSE_ID in the example above
        if (is_first_column) {
          opossum::Chunk chunk(true);
          chunk.add_column(value_column);
          table->add_chunk(std::move(chunk));
        } else {
          auto &chunk = table->get_chunk(chunk_id);
          chunk.add_column(value_column);
        }

        // reset column
        column.clear();
        column.reserve(_chunk_size);
      }
    }

    // write partially filled last chunk
    if (row_count % _chunk_size != 0) {
      auto value_column = std::make_shared<opossum::ValueColumn<T>>(std::move(column));

      // add Chunk if it is the first column, e.g. WAREHOUSE_ID in the example above
      if (is_first_column) {
        opossum::Chunk chunk(true);
        chunk.add_column(value_column);
        table->add_chunk(std::move(chunk));
      } else {
        opossum::ChunkID chunk_id{static_cast<uint32_t>(row_count / _chunk_size)};
        auto &chunk = table->get_chunk(chunk_id);
        chunk.add_column(value_column);
      }
    }
  }
};
}  // namespace benchmark_utilities
