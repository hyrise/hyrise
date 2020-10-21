#pragma once

#include "tpch/tpch_table_generator.hpp"
#include "file_based_table_generator.hpp"

namespace opossum {

// TODO really diamond? Markov?
class JCCHTableGenerator : virtual public AbstractTableGenerator, private TPCHTableGenerator, private FileBasedTableGenerator {
 public:
  // Convenience constructor for creating a JCCHTableGenerator without a benchmarking context
  explicit JCCHTableGenerator(const std::string& dbgen_path, const std::string& data_path, float scale_factor, uint32_t chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a JCCHTableGenerator in a benchmark
  explicit JCCHTableGenerator(const std::string& dbgen_path, const std::string& data_path, float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 protected:
  using TPCHTableGenerator::_indexes_by_table;
  using TPCHTableGenerator::_sort_order_by_table;
  using TPCHTableGenerator::_add_constraints;

  std::string _dbgen_path;
};

}  // namespace opossum
