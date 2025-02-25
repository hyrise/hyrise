#include "jcch_table_generator.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "external_dbgen_utils.hpp"
#include "file_based_table_generator.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

namespace hyrise {

JCCHTableGenerator::JCCHTableGenerator(const std::string& dbgen_path, const std::string& data_path, float scale_factor,
                                       ClusteringConfiguration clustering_configuration, ChunkOffset chunk_size)
    : JCCHTableGenerator(dbgen_path, data_path, scale_factor, clustering_configuration,
                         std::make_shared<BenchmarkConfig>(chunk_size)) {}

JCCHTableGenerator::JCCHTableGenerator(const std::string& dbgen_path, const std::string& data_path, float scale_factor,
                                       ClusteringConfiguration clustering_configuration,
                                       const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config),
      TPCHTableGenerator(scale_factor, clustering_configuration, benchmark_config),
      FileBasedTableGenerator(benchmark_config, data_path + "/"),
      _dbgen_path(dbgen_path) {}

std::unordered_map<std::string, BenchmarkTableInfo> JCCHTableGenerator::generate() {
  auto table_names = std::vector<std::string>{};
  table_names.reserve(tpch_table_names.size());
  for (const auto& [_, table_name] : tpch_table_names) {
    table_names.emplace_back(table_name);
  }

  generate_csv_tables_with_external_dbgen(_dbgen_path, table_names, "resources/benchmark/jcch", _path, _scale_factor,
                                          "-k");

  // Having generated the .csv files, call the FileBasedTableGenerator just as if those files were user-provided
  const auto& generated_tables = FileBasedTableGenerator::generate();

  // FileBasedTableGenerator automatically stores a binary file. Remove the CSV data to save some space.
  remove_csv_tables(_path);

  return generated_tables;
}

void JCCHTableGenerator::_add_constraints(
    std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const {
  TPCHTableGenerator::_add_constraints(table_info_by_name);
}

}  // namespace hyrise
