#include "abstract_table_generator.hpp"

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "operators/export_binary.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

AbstractTableGenerator::AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : _benchmark_config(benchmark_config) {}

void AbstractTableGenerator::generate_and_store() {
  auto table_info_by_name = generate();

  /**
   * Encode the Tables
   */
  for (auto& [table_name, table_info] : table_info_by_name) {
    table_info.re_encoded = BenchmarkTableEncoder::encode(table_name, table_info.table,
                                                          _benchmark_config->encoding_config, _benchmark_config->out);
  }

  /**
   * Write the Tables into binary files if required
   */
  if (_benchmark_config->cache_binary_tables) {
    for (auto& [table_name, table_info] : table_info_by_name) {
      if (!table_info.loaded_from_binary || table_info.re_encoded) {
        auto binary_file_path = std::filesystem::path{};
        if (table_info.binary_file_path) {
          binary_file_path = *table_info.binary_file_path;
        } else {
          binary_file_path = *table_info.text_file_path;
          binary_file_path.replace_extension(".bin");
        }

        _benchmark_config->out << "- Writing '" << table_name << "' into binary file '" << binary_file_path << "'"
                               << std::endl;
        ExportBinary::write_binary(*table_info.table, binary_file_path);
      }
    }
  }

  /**
   * Add the Tables to the StorageManager
   */
  _benchmark_config->out << "- Adding Tables to StorageManager" << std::endl;
  for (auto& [table_name, table_info] : table_info_by_name) {
    StorageManager::get().add_table(table_name, table_info.table);
  }
}

}  // namespace opossum
