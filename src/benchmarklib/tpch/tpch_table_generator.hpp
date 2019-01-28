#pragma once


#include "abstract_table_generator.hpp" // NEEDEDINCLUDE
#include "resolve_data_type.hpp" // NEEDEDINCLUDE
#include "storage/table.hpp" // NEEDEDINCLUDE
#include "storage/value_segment.hpp" // NEEDEDINCLUDE

namespace opossum {

class Chunk;
class Table;

enum class TpchTable { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };

extern std::unordered_map<opossum::TpchTable, std::string> tpch_table_names;

/**
 * Wrapper around the official tpch-dbgen tool, making it directly generate opossum::Table instances without having
 * to generate and then load .tbl files.
 *
 * NOT thread safe because the underlying tpch-dbgen is not (since it has global data and malloc races).
 */
class TpchTableGenerator final : public AbstractTableGenerator {
 public:
  // Convenience constructor for creating a TpchTableGenerator out of a benchmarking context
  explicit TpchTableGenerator(float scale_factor, uint32_t chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a TpchTableGenerator in a benchmark
  explicit TpchTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 private:
  float _scale_factor;
};
}  // namespace opossum
