#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "abstract_pdgf_table_generator.hpp"
#include "file_based_table_generator.hpp"

namespace hyrise {

// Generates the SSB data by calling PDGF.
class SSBTableGenerator : virtual public AbstractPDGFTableGenerator {
 public:
  // Convenience constructor for creating a SSBTableGenerator without a benchmarking context.
  explicit SSBTableGenerator(const std::string& dbgen_path, const std::string& csv_meta_path,
                             const std::string& data_path, float scale_factor,
                             ChunkOffset chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a SSBTableGenerator in a benchmark.
  explicit SSBTableGenerator(const std::string& dbgen_path, const std::string& csv_meta_path,
                             const std::string& data_path, float scale_factor,
                             const std::shared_ptr<BenchmarkConfig>& benchmark_config);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 protected:
  void _add_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const final;
};

}  // namespace hyrise
