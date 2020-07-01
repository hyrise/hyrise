#include "calibration_table_generator.hpp"

#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

namespace opossum {

CalibrationTableGenerator::CalibrationTableGenerator(std::shared_ptr<TableGeneratorConfig> config) {
  // Generate all possible permutations of column types
  for (const auto data_type : config->data_types) {
    for (const auto encoding_type : config->encoding_types) {
      if (encoding_supports_data_type(encoding_type, data_type)) {
        auto column_count = 0;  // Needed to make column names unique
        for (const auto& column_data_distribution : config->column_data_distributions) {
          std::stringstream column_name_stringstream;
          column_name_stringstream << data_type << "_" << encoding_type << "_" << column_count;

          auto column_name = column_name_stringstream.str();

          _column_data_distributions.emplace_back(column_data_distribution);
          _column_specs.emplace_back(
              ColumnSpecification(column_data_distribution, data_type, encoding_type, column_name));

          column_count++;
        }
      }  // if encoding is supported
    }
  }

  _config = config;
}

std::vector<std::shared_ptr<const CalibrationTableWrapper>> CalibrationTableGenerator::generate() const {
  auto table_wrappers = std::vector<std::shared_ptr<const CalibrationTableWrapper>>();
  table_wrappers.reserve(_config->chunk_sizes.size() * _config->row_counts.size());

  auto table_generator = std::make_shared<SyntheticTableGenerator>();

  for (const auto chunk_size : _config->chunk_sizes) {
    for (const auto row_count : _config->row_counts) {
      const auto table = table_generator->generate_table(_column_specs, row_count, chunk_size, UseMvcc::Yes);

      const std::string table_name = std::to_string(chunk_size) + "_" + std::to_string(row_count);

      const auto calibration_table_wrapper = std::make_shared<const CalibrationTableWrapper>(
          CalibrationTableWrapper(table, table_name, _column_data_distributions));
      table_wrappers.emplace_back(calibration_table_wrapper);

      if (_config->generate_sorted_tables) {
        const auto sorted_table_name = table_name + "sorted";
        auto table_wrapper = std::make_shared<TableWrapper>(table);
        table_wrapper->execute();
        std::vector<SortColumnDefinition> sort_column_definition;
        sort_column_definition.emplace_back(SortColumnDefinition{ColumnID{0}});
        auto sort_operator =
            std::make_shared<Sort>(table_wrapper, sort_column_definition, chunk_size, Sort::ForceMaterialization::Yes);
        sort_operator->execute();
        const auto sorted_table = std::const_pointer_cast<Table>(sort_operator->get_output());
        std::vector<ChunkEncodingSpec> chunk_encoding_specs;
        for (auto chunk_id = ChunkID{0}; chunk_id < sorted_table->chunk_count(); ++chunk_id) {
          ChunkEncodingSpec chunk_encoding_spec;
          const auto chunk = sorted_table->get_chunk(chunk_id);
          for (auto segment_id = ColumnID{0}; segment_id < sorted_table->column_count(); ++segment_id) {
            const auto segment = chunk->get_segment(segment_id);
            const auto encoded_segment = std::dynamic_pointer_cast<AbstractEncodedSegment>(segment);
            const auto encoding_type = encoded_segment ? encoded_segment->encoding_type() : EncodingType::Unencoded;
            const auto segment_encoding_spec = SegmentEncodingSpec(encoding_type);
            chunk_encoding_spec.emplace_back(segment_encoding_spec);
          }
          chunk_encoding_specs.emplace_back(chunk_encoding_spec);
        }
        ChunkEncoder::encode_all_chunks(sorted_table, chunk_encoding_specs);
        const auto column_definitions = sorted_table->column_definitions();
        std::vector<std::shared_ptr<Chunk>> chunks;
        for (auto chunk_id = ChunkID{0}; chunk_id < sorted_table->chunk_count(); ++chunk_id) {
          const auto chunk = sorted_table->get_chunk(chunk_id);
          pmr_vector<std::shared_ptr<AbstractSegment>> segments;
          for (auto segment_id = ColumnID{0}; segment_id < sorted_table->column_count(); ++segment_id) {
            segments.emplace_back(chunk->get_segment(segment_id));
          }
          const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
          chunks.emplace_back(std::make_shared<Chunk>(segments, mvcc_data));
        }

        auto sorted_mvcc_table =
            std::make_shared<Table>(column_definitions, TableType::Data, std::move(chunks), UseMvcc::Yes);
        const auto sorted_calibration_table_wrapper = std::make_shared<const CalibrationTableWrapper>(
            CalibrationTableWrapper(sorted_mvcc_table, sorted_table_name, _column_data_distributions));
        table_wrappers.emplace_back(sorted_calibration_table_wrapper);
      }
    }
  }
  return table_wrappers;
}
}  // namespace opossum
