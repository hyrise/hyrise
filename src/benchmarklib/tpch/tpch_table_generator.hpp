#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "benchmark_utilities/abstract_benchmark_table_generator.hpp"

namespace opossum {

class Chunk;
class Table;

enum class TpchTable { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };

extern std::unordered_map<opossum::TpchTable, std::string> tpch_table_names;

/**
 * Wrapper around the official tpch-dbgen tool, making it directly generate opossum::Table instances without having
 * to generate and then load .tbl files.
 *
 * NOT thread safe because the underlying tpch-dbgen is not since it has global data and malloc races.
 */
class TpchTableGenerator : AbstractBenchmarkTableGenerator {
 public:
  explicit TpchTableGenerator(float scale_factor, uint32_t chunk_size = Chunk::MAX_SIZE,
    EncodingConfig config = EncodingConfig{}, bool store = false);

  std::map<std::string, std::shared_ptr<Table>> generate();

  /**
   * Generate the TPCH tables and store them in the StorageManager
   */
  void generate_and_store();

protected:
  std::map<std::string, std::shared_ptr<opossum::Table>> _generate_all_tables() override;

 private:
  float _scale_factor;
};
}  // namespace opossum
