#include "abstract_table_generator.hpp"

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "operators/export_binary.hpp"

namespace opossum {

AbstractTableGenerator::AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config):
  _benchmark_config(benchmark_config) {}

void AbstractTableGenerator::generate_and_store() {
  auto table_entries = _generate();

  /**
   * Encode the Tables
   */
  for (auto& [table_name, table_entry] : table_entries) {
    table_entry.re_encoded = BenchmarkTableEncoder::encode(table_name, table_entry.table, _benchmark_config->encoding_config, _benchmark_config->out);
  }

  /**
   * Write the Tables into binary files if required
   */
  if (_benchmark_config->cache_binary_tables) {
    for (auto& [table_name, table_entry] : table_entries) {
      if (!table_entry.loaded_from_binary || table_entry.re_encoded) {
        auto binary_file_path = std::filesystem::path{};
        if (table_entry.binary_file_path) {
          binary_file_path = *table_entry.binary_file_path;
        } else {
          binary_file_path = *table_entry.text_file_path;
          binary_file_path.replace_extension(".bin");
        }

        _benchmark_config->out << "- Writing '" << table_name << "' into binary file '" << binary_file_path << "'" << std::endl;
        ExportBinary::write_binary(*table_entry.table, binary_file_path);
      }
    }
  }

  /**
   * Add the Tables to the StorageManager
   */
  for (auto& [table_name, table_entry] : table_entries) {
    StorageManager::get().add_table(table_name, table_entry.table);
  }
}

std::shared_ptr<BenchmarkConfig> AbstractTableGenerator::create_minimal_benchmark_config(uint32_t chunk_size) {
  return std::make_shared<BenchmarkConfig>(
    BenchmarkMode::IndividualQueries, true, chunk_size,
   EncodingConfig{}, 0, Duration{}, Duration{}, UseMvcc::No, std::nullopt, false, 1,
  1, false, false, std::cout
  );
}

}  // namespace opossum
