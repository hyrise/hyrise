#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class Table;


    enum Distribution { uniform, normal_skewed, pareto };

    struct ColumnConfiguration {

        Distribution distribution_type = uniform;

        int num_different_values = 1000;

        double pareto_scale = 1.0;
        double pareto_shape = 0.0;

        double skew_location = 0.0;
        double skew_scale = 1.0;
        double skew_shape = 0.0;

        double min_value = 0.0;
        double max_value = 1.0;

    };


class TableGenerator {
public:
    std::shared_ptr<Table> generate_table(const ChunkID chunk_size, const bool compress = false);
    std::shared_ptr<Table> TableGenerator::generate_table(
            const std::vector<ColumnConfiguration> & column_configurations,
            const bool compress, const size_t num_rows, const ChunkID chunk_size);


 protected:
  const size_t _num_columns = 10;
  const size_t _num_rows = 5 * 1000;
  const int _max_different_value = 1000;
};

}  // namespace opossum
