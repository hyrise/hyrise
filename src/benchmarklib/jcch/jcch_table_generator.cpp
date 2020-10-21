#include "jcch_table_generator.hpp"

#include <filesystem>
#include <utility>

namespace opossum {

JCCHTableGenerator::JCCHTableGenerator(const std::string& path, float scale_factor, uint32_t chunk_size)
    : JCCHTableGenerator(path, scale_factor, create_benchmark_config_with_chunk_size(chunk_size)) {}

JCCHTableGenerator::JCCHTableGenerator(const std::string& path, float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config),
    TPCHTableGenerator(scale_factor, benchmark_config),
    FileBasedTableGenerator(benchmark_config, path) {}

std::unordered_map<std::string, BenchmarkTableInfo> JCCHTableGenerator::generate() {
  return FileBasedTableGenerator::generate();
}

}  // namespace opossum
