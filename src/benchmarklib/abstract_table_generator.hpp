#pragma once

#include <chrono>
#include <unordered_map>

#include "encoding_config.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace opossum {

class BenchmarkConfig;

struct BenchmarkTableInfo {
  BenchmarkTableInfo() = default;
  explicit BenchmarkTableInfo(const std::shared_ptr<Table>& table);

  std::shared_ptr<Table> table;

  // Set if the table has a binary/textual file path associated with it. E.g., if the table was loaded from such a file
  // or if it should be exported to it as part of binary caching.
  std::optional<std::filesystem::path> binary_file_path;
  std::optional<std::filesystem::path> text_file_path;

  bool loaded_from_binary{false};

  // True IFF there are both a binary and a text file for this file AND the text file was written to more recently than
  // the binary file. We take this as the text file being changed and re-export the binary
  bool binary_file_out_of_date{false};

  // True, if the encoding of the table needed to be changed after loading it in order to satisfy the benchmark's
  // encoding configuration. False, if the benchmark was loaded in the requested encoding. Note that binary table files
  // support encodings, so it is possible that the benchmark is run with, e.g. Dictionary encoding, and the tables are
  // available as binary files with that encoding.
  bool re_encoded{false};
};

struct TableGenerationMetrics {
  std::chrono::nanoseconds generation_duration{};
  std::chrono::nanoseconds encoding_duration{};
  std::chrono::nanoseconds binary_caching_duration{};
  std::chrono::nanoseconds sort_duration{};
  std::chrono::nanoseconds store_duration{};
  std::chrono::nanoseconds index_duration{};
};

void to_json(nlohmann::json& json, const TableGenerationMetrics& metrics);

class AbstractTableGenerator {
 public:
  explicit AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config);
  virtual ~AbstractTableGenerator() = default;

  void generate_and_store();

  /**
   * @return A table_name -> TableEntry mapping
   */
  virtual std::unordered_map<std::string, BenchmarkTableInfo> generate() = 0;

  TableGenerationMetrics metrics;

  static std::shared_ptr<BenchmarkConfig> create_benchmark_config_with_chunk_size(ChunkOffset chunk_size);

 protected:
  // Creates indexes, expects the table to have been added to the StorageManager and, if requested, encoded
  using IndexesByTable = std::map<std::string, std::vector<std::vector<std::string>>>;
  virtual IndexesByTable _indexes_by_table() const;

  // Optionally, the benchmark may define tables (left side) that are ordered (aka. clustered) by one of their columns
  // (right side).
  using SortOrderByTable = std::map<std::string, std::string>;
  virtual SortOrderByTable _sort_order_by_table() const;

  const std::shared_ptr<BenchmarkConfig> _benchmark_config;
};

}  // namespace opossum
