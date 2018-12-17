#pragma once

#include <unordered_map>

#include "encoding_config.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace opossum {

class BenchmarkConfig;

struct BenchmarkTableInfo {
  std::shared_ptr<Table> table;

  // Set if the table has a binary/textual file path associated with it. E.g., if the table was loaded from such a file
  // or if it should be exported to it as part of binary caching.
  std::optional<std::filesystem::path> binary_file_path;
  std::optional<std::filesystem::path> text_file_path;

  bool loaded_from_binary{false};

  // True, if the encoding of the table needed to be changed after loading it in order to satisfy the benchmark's
  // encoding configuration. False, if the benchmark was loaded in the requested encoding. Note that binary table files
  // support encodings, so it is possible that the benchmark is run with, e.g. Dictionary encoding, and the tables are
  // available as binary files with that encoding.
  bool re_encoded{false};
};

class AbstractTableGenerator {
 public:
  explicit AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config);
  virtual ~AbstractTableGenerator() = default;

  void generate_and_store();

  /**
   * @return A table_name -> TableEntry mapping
   */
  virtual std::unordered_map<std::string, BenchmarkTableInfo> generate() = 0;

 protected:
  /**
   * The AbstractTableGenerator stores the whole BenchmarkConfig, yet not all fields in BenchmarkConfig influence
   * table generation.
   * This helper function creates a BenchmarkConfig with the fields that affect table generation
   */
  static std::shared_ptr<BenchmarkConfig> _create_minimal_benchmark_config(uint32_t chunk_size);

  const std::shared_ptr<BenchmarkConfig> _benchmark_config;
};

}  // namespace opossum
