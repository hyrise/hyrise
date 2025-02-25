#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <set>

#include "abstract_pdgf_table_generator.hpp"
#include "abstract_table_generator.hpp"
#include "storage/chunk.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "types.hpp"

namespace hyrise {

class Chunk;
class Table;

/**
 * Generating table by invoking the external PDGF data generator and pulling the results into Hyrise via shared memory.
 */
class TPCHPDGFTableGenerator : virtual public AbstractPDGFTableGenerator {
 public:
  // Convenience constructor for creating a TPCHPDGFTableGenerator without a benchmarking context
  explicit TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                              ChunkOffset chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a TPCHPDGFTableGenerator in a benchmark
  explicit TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                  const std::shared_ptr<BenchmarkConfig>& benchmark_config, std::vector<std::pair<BenchmarkItemID, std::string>> queries_to_run);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 protected:
  std::string _benchmark_name_short() const override;
  std::string _pdgf_schema_config_file() const override;
  uint32_t _pdgf_buffer_columns() const override {
    return 16u;
  }
  IndexesByTable _indexes_by_table() const override;
  SortOrderByTable _sort_order_by_table() const override;
  void _add_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const override;

  ClusteringConfiguration _clustering_configuration;
};
}  // namespace hyrise
