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
  std::cout << "- Loading/Generating tables done (" << timer.lap_formatted() << ")" << std::endl;

  /**
   * Encode the Tables
   */
  std::cout << "- Encoding tables if necessary" << std::endl;
  for (auto& [table_name, table_info] : table_info_by_name) {
    std::cout << "-  Encoding '" << table_name << "' - " << std::flush;
    Timer per_table_timer;
    table_info.re_encoded =
        BenchmarkTableEncoder::encode(table_name, table_info.table, _benchmark_config->encoding_config);
    std::cout << (table_info.re_encoded ? "encoding applied" : "no encoding necessary");
    std::cout << " (" << per_table_timer.lap_formatted() << ")" << std::endl;
  }
  std::cout << "- Encoding tables done (" << timer.lap_formatted() << ")" << std::endl;

  /**
   * Write the Tables into binary files if required
   */
  if (_benchmark_config->cache_binary_tables) {
    std::cout << "- Writing tables into binary files if necessary" << std::endl;

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
      Timer per_table_timer;
      ExportBinary::write_binary(*table_info.table, binary_file_path);
      std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
    }
    std::cout << "- Writing tables into binary files done (" << timer.lap_formatted() << ")" << std::endl;
  }

  /**
   * Add the Tables to the StorageManager
   */
  std::cout << "- Adding Tables to StorageManager and generating statistics " << std::endl;
  auto& storage_manager = StorageManager::get();
  for (auto& [table_name, table_info] : table_info_by_name) {
    std::cout << "-  Adding '" << table_name << "' " << std::flush;
    Timer per_table_timer;
    if (storage_manager.has_table(table_name)) storage_manager.drop_table(table_name);
    storage_manager.add_table(table_name, table_info.table);
    std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
  }

  std::cout << "- Adding Tables to StorageManager and generating statistics done (" << timer.lap_formatted() << ")"
            << std::endl;
}

}  // namespace opossum
