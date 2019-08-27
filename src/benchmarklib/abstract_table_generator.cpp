#include "abstract_table_generator.hpp"

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "hyrise.hpp"
#include "operators/export_binary.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace opossum {

void to_json(nlohmann::json& json, const TableGenerationMetrics& metrics) {
  json = {{"generation_duration", metrics.generation_duration.count()},
          {"encoding_duration", metrics.encoding_duration.count()},
          {"binary_caching_duration", metrics.binary_caching_duration.count()},
          {"store_duration", metrics.store_duration.count()},
          {"index_duration", metrics.index_duration.count()}};
}

BenchmarkTableInfo::BenchmarkTableInfo(const std::shared_ptr<Table>& table) : table(table) {}

AbstractTableGenerator::AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : _benchmark_config(benchmark_config) {}

void AbstractTableGenerator::generate_and_store() {
  Timer timer;

  std::cout << "- Loading/Generating tables " << std::endl;
  auto table_info_by_name = generate();
  metrics.generation_duration = timer.lap();
  std::cout << "- Loading/Generating tables done (" << format_duration(metrics.generation_duration) << ")" << std::endl;

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
  metrics.encoding_duration = timer.lap();
  std::cout << "- Encoding tables done (" << format_duration(metrics.encoding_duration) << ")" << std::endl;

  /**
   * Write the Tables into binary files if required
   */
  if (_benchmark_config->cache_binary_tables) {
    for (auto& [table_name, table_info] : table_info_by_name) {
      const auto& table = table_info.table;
      if (table->chunk_count() > 1 && table->get_chunk(ChunkID{0})->size() != _benchmark_config->chunk_size) {
        std::cout << "- WARNING: " << table_name << " was loaded from binary, but has a mismatching chunk size of "
                  << table->get_chunk(ChunkID{0})->size() << std::endl;
      }
    }

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

      std::cout << "- Writing '" << table_name << "' into binary file " << binary_file_path << " " << std::flush;
      Timer per_table_timer;
      ExportBinary::write_binary(*table_info.table, binary_file_path);
      std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
    }
    metrics.binary_caching_duration = timer.lap();
    std::cout << "- Writing tables into binary files done (" << format_duration(metrics.binary_caching_duration) << ")"
              << std::endl;
  }

  /**
   * Add the Tables to the StorageManager
   */
  std::cout << "- Adding tables to StorageManager and generating statistics" << std::endl;
  auto& storage_manager = Hyrise::get().storage_manager;
  for (auto& [table_name, table_info] : table_info_by_name) {
    std::cout << "-  Adding '" << table_name << "' " << std::flush;
    Timer per_table_timer;
    if (storage_manager.has_table(table_name)) storage_manager.drop_table(table_name);
    storage_manager.add_table(table_name, table_info.table);
    std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
  }

  metrics.store_duration = timer.lap();

  std::cout << "- Adding tables to StorageManager and generating statistics done ("
            << format_duration(metrics.store_duration) << ")" << std::endl;

  std::cout << "- Creating indexes" << std::endl;
  for (const auto& [table_name, indexes] : _indexes_by_table()) {
    const auto& table = table_info_by_name[table_name].table;

    for (const auto& index_columns : indexes) {
      auto column_ids = std::vector<ColumnID>{};

      for (const auto& index_column : index_columns) {
        column_ids.emplace_back(table->column_id_by_name(index_column));
      }

      std::cout << "-  Creating index on " << table_name << " [ ";
      for (const auto& index_column : index_columns) {
        std::cout << index_column << " ";
      }
      std::cout << "] " << std::flush;
      Timer per_index_timer;

      if (column_ids.size() == 1) {
        table->create_index<GroupKeyIndex>(column_ids);
      } else {
        table->create_index<CompositeGroupKeyIndex>(column_ids);
      }

      std::cout << "(" << per_index_timer.lap_formatted() << ")" << std::endl;
    }
  }
  metrics.index_duration = timer.lap();
  std::cout << "- Creating indexes done (" << format_duration(metrics.index_duration) << ")" << std::endl;
}

std::shared_ptr<BenchmarkConfig> AbstractTableGenerator::create_benchmark_config_with_chunk_size(
    ChunkOffset chunk_size) {
  auto config = BenchmarkConfig::get_default_config();
  config.chunk_size = chunk_size;
  return std::make_shared<BenchmarkConfig>(config);
}

AbstractTableGenerator::IndexesByTable AbstractTableGenerator::_indexes_by_table() const { return {}; };

}  // namespace opossum
