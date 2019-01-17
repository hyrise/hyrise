#include "abstract_table_generator.hpp"

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "operators/export_binary.hpp"
#include "storage/storage_manager.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace opossum {

AbstractTableGenerator::AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : _benchmark_config(benchmark_config) {}

void AbstractTableGenerator::generate_and_store() {
  Timer timer;

  std::cout << "- Loading/Generating tables " << std::endl;
  auto table_info_by_name = generate();

  /**
   * Encode the Tables
   */
  std::cout << "- Encoding tables if necessary" << std::endl;
  for (auto& [table_name, table_info] : table_info_by_name) {
    timer.lap();
    std::cout << "-  Encoding '" << table_name << "' " << std::flush;
    table_info.re_encoded =
        BenchmarkTableEncoder::encode(table_name, table_info.table, _benchmark_config->encoding_config);
    std::cout << (table_info.re_encoded ? " - no encoding necessary " : " - encoding applied ");
    std::cout << "(" << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap())) << ")"
              << std::endl;
  }

  /**
   * Write the Tables into binary files if required
   */
  if (_benchmark_config->cache_binary_tables) {
    timer.lap();

    for (auto& [table_name, table_info] : table_info_by_name) {
      if (table_info.loaded_from_binary && !table_info.re_encoded && !table_info.binary_file_out_of_date) {
        continue;
      }

      auto binary_file_path = std::filesystem::path{};
      if (table_info.binary_file_path) {
        binary_file_path = *table_info.binary_file_path;
      } else {
        binary_file_path = *table_info.text_file_path;
        binary_file_path.replace_extension(".bin");
      }

      std::cout << "- Writing '" << table_name << "' into binary file '" << binary_file_path << "' " << std::flush;
      ExportBinary::write_binary(*table_info.table, binary_file_path);
      std::cout << "(" << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap())) << ")"
                << std::endl;
    }
  }

  /**
   * Add the Tables to the StorageManager
   */
  std::cout << "- Adding Tables to StorageManager and generating statistics " << std::flush;
  auto& storage_manager = StorageManager::get();
  for (auto& [table_name, table_info] : table_info_by_name) {
    if (storage_manager.has_table(table_name)) storage_manager.drop_table(table_name);
    storage_manager.add_table(table_name, table_info.table);
  }

  std::cout << "(" << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap())) << ")"
            << std::endl;
}

}  // namespace opossum
