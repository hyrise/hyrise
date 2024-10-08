#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_table_generator.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "tpch/tpch_constants.hpp"
#include "types.hpp"

namespace hyrise {

class Chunk;
class Table;

enum class TPCHTable { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };

extern const std::unordered_map<TPCHTable, std::string> tpch_table_names;

/**
 * Generating table by invoking the external PDGF data generator and pulling the results into Hyrise via shared memory.
 */
class TPCHPDGFTableGenerator : virtual public AbstractTableGenerator {
 public:
  // Convenience constructor for creating a TPCHPDGFTableGenerator without a benchmarking context
  explicit TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                              ChunkOffset chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a TPCHPDGFTableGenerator in a benchmark
  explicit TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                              const std::shared_ptr<BenchmarkConfig>& benchmark_config);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 protected:
  IndexesByTable _indexes_by_table() const override;
  SortOrderByTable _sort_order_by_table() const override;
  void _add_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const override;

  const float _scale_factor;
  const ClusteringConfiguration _clustering_configuration;
};
}  // namespace hyrise
