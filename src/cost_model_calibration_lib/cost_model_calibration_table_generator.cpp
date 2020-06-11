#include "cost_model_calibration_table_generator.hpp"

#include "hyrise.hpp"
#include "query/calibration_query_generator.hpp"
#include "storage/encoding_type.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "synthetic_table_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/load_table.hpp"

#include "operators/print.hpp"

namespace opossum {

CostModelCalibrationTableGenerator::CostModelCalibrationTableGenerator(const CalibrationConfiguration configuration,
                                                                       const ChunkOffset chunk_size)
    : _chunk_size(chunk_size), _configuration(configuration) {}

void CostModelCalibrationTableGenerator::load_calibration_tables() const {
  const auto table_specifications = _configuration.table_specifications;

  for (const auto& table_specification : table_specifications) {
    std::cout << "Loading table " << table_specification.table_name << std::endl;
    const auto table = load_table(table_specification.table_path, _chunk_size);
    std::cout << "Loaded table " << table_specification.table_name << " successfully." << std::endl;

    ChunkEncodingSpec chunk_spec;

    const auto& column_specifications = _configuration.columns;
    for (const auto& column_specification : column_specifications) {
      chunk_spec.push_back(SegmentEncodingSpec{column_specification.encoding});
    }

    ChunkEncoder::encode_all_chunks(table, chunk_spec);

    const auto column_count = table->column_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        std::vector<ColumnID> column_ids{};
        column_ids.push_back(column_id);
        chunk->template create_index<BTreeIndex>(column_ids);
      }
    }

    Hyrise::get().storage_manager.add_table(table_specification.table_name, table);

    std::cout << "Encoded table " << table_specification.table_name << " successfully." << std::endl;
  }
}

void CostModelCalibrationTableGenerator::generate_calibration_tables() const {
  SyntheticTableGenerator table_generator;

  std::vector<ColumnSpecification> column_specifications;
  column_specifications.reserve(_configuration.columns.size());

  for (const auto& column_spec : _configuration.columns) {
    //column_specifications.emplace_back(ColumnSpecification(ColumnDataDistribution::make_uniform_config(0.0, column_spec.distinct_value_count),
//		                        column_spec.data_type, column_spec.encoding, column_spec.column_name));
    column_specifications.emplace_back(ColumnDataDistribution::make_uniform_config(0.0, static_cast<double>(column_spec.distinct_value_count)),
		                       column_spec.data_type, column_spec.encoding, column_spec.column_name);
  }

  for (const auto table_size : _configuration.table_generation_table_sizes) {
    auto const table_name = _configuration.table_generation_name_prefix + std::to_string(table_size);

    std::cout << "Table >>" << table_name << "<<\tdata generation: " << std::flush;
    auto table = table_generator.generate_table(column_specifications, table_size, _chunk_size, UseMvcc::Yes);

    std::cout << "done -- adding to storage manager: " << std::flush;

    Hyrise::get().storage_manager.add_table(table_name, table);
    std::cout << "done -- creating indexes: " << std::flush;

    for (const auto& column_spec : _configuration.columns) {
      if (column_spec.encoding.encoding_type == EncodingType::Dictionary &&
          *column_spec.encoding.vector_compression_type == VectorCompressionType::FixedSizeByteAligned) {
        std::vector<ColumnID> column_ids{};
        column_ids.push_back(column_spec.column_id);
        table->create_index<GroupKeyIndex>(column_ids);
      }
    }
    std::cout << "done." << std::endl;
  }
}

void CostModelCalibrationTableGenerator::load_tpch_tables(const float scale_factor, const EncodingType encoding) const {
  const auto tables = opossum::TPCHTableGenerator(scale_factor, _chunk_size).generate();

  for (auto& tpch_table : tables) {
    const auto& table_name = tpch_table.first;
    const auto& table = tpch_table.second.table;

    //    if (table_name != "lineitem") continue;

    ChunkEncodingSpec chunk_spec;
    const auto column_count = table->column_count();
    for (auto column_id = ColumnCount{0}; column_id < column_count; ++column_id) {
      chunk_spec.push_back(encoding);
    }

    ChunkEncoder::encode_all_chunks(table, chunk_spec);
    auto& storage_manager = Hyrise::get().storage_manager;

    if (storage_manager.has_table(table_name)) {
      storage_manager.drop_table(table_name);
    }

    storage_manager.add_table(table_name, table);

    std::cout << "Encoded table " << table_name << " successfully." << std::endl;
  }
}

}  // namespace opossum
