#include "cost_model_calibration_table_generator.hpp"

#include "query/calibration_query_generator.hpp"
#include "storage/encoding_type.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "table_generator.hpp"
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
      auto chunk = table->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        std::vector<ColumnID> column_ids{};
        column_ids.push_back(column_id);
        chunk->template create_index<BTreeIndex>(column_ids);
      }
    }

    StorageManager::get().add_table(table_specification.table_name, table);

    std::cout << "Encoded table " << table_specification.table_name << " successfully." << std::endl;
  }
}

void CostModelCalibrationTableGenerator::generate_calibration_tables() const {
  TableGenerator table_generator;

  // Gather data required for table generator.
  std::vector<ColumnDataDistribution> column_data_distributions;
  std::vector<DataType> column_data_types;
  std::vector<std::string> column_names;
  std::vector<SegmentEncodingSpec> column_encodings;

  if (_configuration.calibrate_joins) {
    struct TableDef {
      size_t table_size;
      std::vector<float> distinctnesses;
    };

    std::vector<TableDef> table_defs_S = {{10, std::vector<float>{1.0f, 0.1f}}, {1'000, std::vector<float>{1.0f, 0.1f, 0.01f}}, {10'000, std::vector<float>{1.0f, 0.1f, 0.01f, 0.001f}}, {100'000, std::vector<float>{1.0f, 0.1f, 0.01f, 0.001f, 0.0001f}}, {1'000'000, std::vector<float>{1.0f, 0.1f, 0.01f, 0.001f}}};
    std::vector<TableDef> table_defs_R = {{10, std::vector<float>{1.0f, 0.1f}}, {1'000, std::vector<float>{1.0f, 0.1f, 0.01f}}, {10'000, std::vector<float>{1.0f, 0.1f, 0.01f, 0.001f}}, {100'000, std::vector<float>{1.0f, 0.1f, 0.01f}}, {1'000'000, std::vector<float>{1.0f, 0.1f, 0.01f}}};

    for (const auto& table_def_S : table_defs_S) {
      std::vector<ColumnDataDistribution> column_data_distributions_S;
      std::vector<DataType> column_data_types_S;
      std::vector<std::string> column_names_S;
      std::vector<SegmentEncodingSpec> column_encodings_S;

      for (const auto distinctness : table_def_S.distinctnesses) {
        size_t distinct_value_count = static_cast<size_t>(table_def_S.table_size * distinctness);
        Assert(distinct_value_count <= table_def_S.table_size, "Dist table size mismatch");

        column_data_distributions_S.push_back(ColumnDataDistribution::make_uniform_config(0.0, distinct_value_count));

        column_data_types_S.push_back(DataType::Int);

        column_encodings_S.push_back(SegmentEncodingSpec{EncodingType::Dictionary});

        column_names_S.push_back("S_" + std::to_string(table_def_S.table_size) + "_" + std::to_string(distinct_value_count));
      }

      const auto table_name_S = _configuration.table_generation_name_prefix + std::to_string(table_def_S.table_size) + "_S";

      auto table = table_generator.generate_table(column_data_distributions_S, column_data_types_S, table_def_S.table_size, _chunk_size,
                                                  column_encodings_S, column_names_S, UseMvcc::Yes, false);
      StorageManager::get().add_table(table_name_S, table);

      std::cout << "Table " << table_name_S << " done " << std::endl;
    }

    for (const auto& table_def_R : table_defs_R) {
      std::vector<ColumnDataDistribution> column_data_distributions_R;
      std::vector<DataType> column_data_types_R;
      std::vector<std::string> column_names_R;
      std::vector<SegmentEncodingSpec> column_encodings_R;

      for (const auto distinctness : table_def_R.distinctnesses) {
        size_t distinct_value_count = static_cast<size_t>(table_def_R.table_size * distinctness);
        Assert(distinct_value_count <= table_def_R.table_size, "Dist table size mismatch");

        column_data_distributions_R.push_back(ColumnDataDistribution::make_uniform_config(0.0, distinct_value_count));

        column_data_types_R.push_back(DataType::Int);

        column_encodings_R.push_back(SegmentEncodingSpec{EncodingType::Dictionary});

        column_names_R.push_back("R_" + std::to_string(table_def_R.table_size) + "_" + std::to_string(distinct_value_count));
      }

      const auto table_name_R = _configuration.table_generation_name_prefix + std::to_string(table_def_R.table_size) + "_R";

      auto table = table_generator.generate_table(column_data_distributions_R, column_data_types_R, table_def_R.table_size, _chunk_size,
                                                  column_encodings_R, column_names_R, UseMvcc::Yes, false);
      StorageManager::get().add_table(table_name_R, table);

      std::cout << "Table " << table_name_R << " done " << std::endl;
    }

    return;
  }


  for (const auto& column_spec : _configuration.columns) {
    column_data_distributions.push_back(
        ColumnDataDistribution::make_uniform_config(0.0, column_spec.distinct_value_count));
    column_data_types.push_back(column_spec.data_type);
    column_names.push_back(column_spec.column_name);
    column_encodings.push_back(column_spec.encoding);
  }

  for (const auto table_size : _configuration.table_generation_table_sizes) {
    auto const table_name = _configuration.table_generation_name_prefix + std::to_string(table_size);

    std::cout << "Table >>" << table_name << "<<\tdata generation: " << std::flush;
    auto table = table_generator.generate_table(column_data_distributions, column_data_types, table_size, _chunk_size,
                                                column_encodings, column_names, UseMvcc::Yes, false);

    std::cout << "done -- adding to storage manager: " << std::flush;

    StorageManager::get().add_table(table_name, table);
    std::cout << "done -- creating indexes: " << std::flush;

    for (const auto& column_spec : _configuration.columns) {
      if (column_spec.encoding.encoding_type == EncodingType::Dictionary &&
          *column_spec.encoding.vector_compression_type == VectorCompressionType::FixedSizeByteAligned) {
        // std::cout << column_spec.column_id << " is a dict fsba" << std::endl;
        table->template create_index<GroupKeyIndex>({column_spec.column_id});
      }
    }
    std::cout << " done." << std::endl;
  }
}

void CostModelCalibrationTableGenerator::load_tpch_tables(const float scale_factor, const EncodingType encoding) const {
  const auto tables = opossum::TpchTableGenerator(scale_factor, _chunk_size).generate();

  for (auto& tpch_table : tables) {
    const auto& table_name = tpch_table.first;
    const auto& table = tpch_table.second.table;

    //    if (table_name != "lineitem") continue;

    ChunkEncodingSpec chunk_spec;
    const auto column_count = table->column_count();
    for (size_t i = 0; i < column_count; ++i) {
      chunk_spec.push_back(encoding);
    }

    ChunkEncoder::encode_all_chunks(table, chunk_spec);
    auto& storage_manager = StorageManager::get();

    if (storage_manager.has_table(table_name)) {
      storage_manager.drop_table(table_name);
    }

    storage_manager.add_table(table_name, table);

    std::cout << "Encoded table " << table_name << " successfully." << std::endl;
  }
}

}  // namespace opossum
