#pragma once

#include <unordered_map>

#include "encoding_config.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace opossum {

class BenchmarkConfig;

class AbstractTableGenerator {
 public:
  explicit AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config);
  virtual ~AbstractTableGenerator() = default;

  void generate_and_store();

  struct TableEntry {
    std::optional<std::filesystem::path> binary_file_path;
    std::optional<std::filesystem::path> text_file_path;
    bool loaded_from_binary{false};
    bool re_encoded{false};
    std::shared_ptr<Table> table;
  };

  /**
   * @return A table_name -> TableEntry mapping
   */
  virtual std::unordered_map<std::string, TableEntry> generate() = 0;

 protected:
  /**
   * The AbstractTableGenerator stores the whole BenchmarkConfig, yet not all fields in BenchmarkConfig influence
   * table generation.
   * This helper function creates a BenchmarkConfig with the fields that affect table generation
   */
  static std::shared_ptr<BenchmarkConfig> create_minimal_benchmark_config(uint32_t chunk_size);

  const std::shared_ptr<BenchmarkConfig> _benchmark_config;
};

}  // namespace opossum
