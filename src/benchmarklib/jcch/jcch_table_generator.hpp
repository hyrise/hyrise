#pragma once

#include "file_based_table_generator.hpp"
#include "tpch/tpch_table_generator.hpp"

namespace opossum {

// Generates the JCC-H data by calling JCC-H's dbgen binary. See jcch_benchmark.cpp for details.
// This uses multiple inheritance from TPCHTableGenerator (for the sort order, indexes, and constraints) and from
// FileBasedTableGenerator (for the csv loading part). One could argue if composition would be more appropriate
// here. The relationship between FileBasedTableGenerator and JCCHTableGenerator does not really satisfy the Liskov
// substitution principle. However, it makes reusing the TPC-H definitions much easier.

class JCCHTableGenerator : virtual public AbstractTableGenerator,
                           private TPCHTableGenerator,
                           private FileBasedTableGenerator {
 public:
  // Convenience constructor for creating a JCCHTableGenerator without a benchmarking context
  explicit JCCHTableGenerator(const std::string& dbgen_path, const std::string& data_path, float scale_factor,
                              uint32_t chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a JCCHTableGenerator in a benchmark
  explicit JCCHTableGenerator(const std::string& dbgen_path, const std::string& data_path, float scale_factor,
                              const std::shared_ptr<BenchmarkConfig>& benchmark_config);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 protected:
  using TPCHTableGenerator::_add_constraints;
  using TPCHTableGenerator::_indexes_by_table;
  using TPCHTableGenerator::_sort_order_by_table;

  std::string _dbgen_path;
};

}  // namespace opossum
