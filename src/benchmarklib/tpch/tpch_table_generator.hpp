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
#include "tpch_constants.hpp"
#include "types.hpp"

namespace hyrise {

class Chunk;
class Table;

enum class TPCHTable { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };

extern const std::unordered_map<TPCHTable, std::string> tpch_table_names;

/**
 * Wrapper around the official tpch-dbgen tool, making it directly generate hyrise::Table instances without having
 * to generate and then load .tbl files.
 *
 * NOT thread safe because the underlying tpch-dbgen is not (since it has global data and malloc races).
 */
class TPCHTableGenerator : virtual public AbstractTableGenerator {
 public:
  // Convenience constructor for creating a TPCHTableGenerator without a benchmarking context
  explicit TPCHTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                              ChunkOffset chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a TPCHTableGenerator in a benchmark
  explicit TPCHTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
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
