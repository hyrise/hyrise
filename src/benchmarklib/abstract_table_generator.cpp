#include "abstract_table_generator.hpp"

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "operators/export_binary.hpp"
#include "storage/storage_manager.hpp"
#include "utils/timer.hpp"

namespace opossum {

void to_json(nlohmann::json& json, const TableGenerationMetrics& metrics) {
  json = {
  "generation_duration", metrics.generation_duration.count(),
  "encoding_duration", metrics.encoding_duration.count(),
  "binary_caching_duration", metrics.binary_caching_duration.count(),
  "store_duration", metrics.store_duration.count()
  };
}

AbstractTableGenerator::AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : _benchmark_config(benchmark_config) {}

void AbstractTableGenerator::generate_and_store() {
  auto metrics_timer = Timer{};

  auto table_info_by_name = generate();

  metrics.generation_duration = metrics_timer.lap();

  /**
   * Encode the Tables
   */
  for (auto& [table_name, table_info] : table_info_by_name) {
    table_info.re_encoded = BenchmarkTableEncoder::encode(table_name, table_info.table,
                                                          _benchmark_config->encoding_config, _benchmark_config->out);
  }

  metrics.encoding_duration = metrics_timer.lap();

  /**
   * Write the Tables into binary files if required
   */
  if (_benchmark_config->cache_binary_tables) {
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

      _benchmark_config->out << "- Writing '" << table_name << "' into binary file '" << binary_file_path << "'"
                             << std::endl;
      ExportBinary::write_binary(*table_info.table, binary_file_path);
    }
  }

  metrics.binary_caching_duration = metrics_timer.lap();

  /**
   * Add the Tables to the StorageManager
   */
  _benchmark_config->out << "- Adding Tables to StorageManager" << std::endl;
  auto& storage_manager = StorageManager::get();
  for (auto& [table_name, table_info] : table_info_by_name) {
    if (storage_manager.has_table(table_name)) storage_manager.drop_table(table_name);
    storage_manager.add_table(table_name, table_info.table);
  }

  metrics.store_duration = metrics_timer.lap();
}

}  // namespace opossum
