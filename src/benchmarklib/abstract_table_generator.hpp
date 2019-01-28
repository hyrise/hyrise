#pragma once


#include "encoding_config.hpp" // NEEDEDINCLUDE

namespace opossum {

class BenchmarkConfig;

struct BenchmarkTableInfo {
  std::shared_ptr<Table> table;

  // Set if the table has a binary/textual file path associated with it. E.g., if the table was loaded from such a file
  // or if it should be exported to it as part of binary caching.
  std::optional<std::string> binary_file_path;
  std::optional<std::string> text_file_path;

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
