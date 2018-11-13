#include "cost_model_calibration_table_generator.hpp"

#include "query/calibration_query_generator.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "utils/load_table.hpp"

namespace opossum {

CostModelCalibrationTableGenerator::CostModelCalibrationTableGenerator(const CalibrationConfiguration configuration,
                                                                       const size_t chunk_size)
    : _chunk_size(chunk_size), _configuration(configuration) {}

void CostModelCalibrationTableGenerator::load_calibration_tables() const {
  const auto table_specifications = _configuration.table_specifications;

  for (const auto& table_specification : table_specifications) {
    std::cout << "Loading table " << table_specification.table_name << std::endl;
    const auto table = load_table(table_specification.table_path, _chunk_size);
    std::cout << "Loaded table " << table_specification.table_name << " successfully." << std::endl;

    ChunkEncodingSpec chunk_spec;

    // Need to iterate the table's column_names because column_specifications is an unordered map
    const auto& column_specifications = table_specification.columns;
    for (const auto& column_name : table->column_names()) {
      const auto& column_specification = column_specifications.at(column_name);
      chunk_spec.push_back(column_specification.encoding);
    }

    ChunkEncoder::encode_all_chunks(table, chunk_spec);
    StorageManager::get().add_table(table_specification.table_name, table);

    std::cout << "Encoded table " << table_specification.table_name << " successfully." << std::endl;
  }
}

void CostModelCalibrationTableGenerator::load_tpch_tables(const float scale_factor) const {
  const auto tables = opossum::TpchDbGenerator(scale_factor, _chunk_size).generate();

  for (auto& tpch_table : tables) {
    const auto& table_name = opossum::tpch_table_names.at(tpch_table.first);
    const auto& table = tpch_table.second;

    // Default is DictionaryEncoding
    ChunkEncodingSpec chunk_spec(table->column_count());
    ChunkEncoder::encode_all_chunks(table, chunk_spec);
    StorageManager::get().add_table(table_name, table);

    std::cout << "Encoded table " << table_name << " successfully." << std::endl;
  }
}

}  // namespace opossum
