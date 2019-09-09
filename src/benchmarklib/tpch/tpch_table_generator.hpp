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
#include "types.hpp"

namespace opossum {

class Chunk;
class Table;

enum class TPCHTable { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };

extern std::unordered_map<opossum::TPCHTable, std::string> tpch_table_names;

/**
 * Wrapper around the official tpch-dbgen tool, making it directly generate opossum::Table instances without having
 * to generate and then load .tbl files.
 *
 * NOT thread safe because the underlying tpch-dbgen is not (since it has global data and malloc races).
 */
class TPCHTableGenerator final : public AbstractTableGenerator {
 public:
  // Convenience constructor for creating a TPCHTableGenerator out of a benchmarking context
  explicit TPCHTableGenerator(float scale_factor, uint32_t chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a TPCHTableGenerator in a benchmark
  explicit TPCHTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 protected:
  IndexesByTable _indexes_by_table() const override;
  SortOrderByTable _sort_order_by_table() const override;

 private:
  float _scale_factor;
};
}  // namespace opossum
