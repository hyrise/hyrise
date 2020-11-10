#include "calibration_table_generator.hpp"

#include <unordered_map>

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
  auto min_table_count = _config->chunk_sizes.size() * _config->row_counts.size();
  if (_config->generate_sorted_tables) {
    min_table_count += min_table_count * _column_data_distributions.size() / _config->column_data_distributions.size();
  }

  auto table_wrappers = std::vector<std::shared_ptr<const CalibrationTableWrapper>>();
  table_wrappers.reserve(min_table_count);
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();
  int max_distinct_values{0};
  std::for_each(_column_data_distributions.begin(), _column_data_distributions.end(),
                [&max_distinct_values](auto& column_distribution) {
                  const auto value_range =
                      static_cast<int>(column_distribution.max_value - column_distribution.min_value);
                  if (value_range > max_distinct_values) max_distinct_values = value_range;
                });

  for (const auto chunk_size : _config->chunk_sizes) {
    for (const auto row_count : _config->row_counts) {
      const auto table = table_generator->generate_table(_column_specs, row_count, chunk_size, UseMvcc::Yes);

      const std::string table_name = std::to_string(chunk_size) + "_" + std::to_string(row_count);

      const auto calibration_table_wrapper =
          std::make_shared<const CalibrationTableWrapper>(table, table_name, _column_data_distributions);
      table_wrappers.emplace_back(calibration_table_wrapper);

      if (_config->generate_sorted_tables) {
        table_wrappers.emplace_back(_generate_sorted_table(calibration_table_wrapper));
      }

      if (_config->generate_foreign_key_tables && row_count <= _config->foreign_key_threshold &&
          row_count > max_distinct_values) {
        table_wrappers.emplace_back(_generate_foreign_key_table(calibration_table_wrapper, table_generator));
      }
    }
  }
  return table_wrappers;
}

std::shared_ptr<const CalibrationTableWrapper> CalibrationTableGenerator::_generate_sorted_table(
    const std::shared_ptr<const CalibrationTableWrapper>& original_table) const {
  const auto sorted_table_name = original_table->get_name() + "_sorted";
  const auto table = original_table->get_table();
  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  std::unordered_map<ChunkID, Segments> segments;
  ChunkEncodingSpec chunk_encoding_spec;
  std::vector<ColumnDataDistribution> column_data_distributions;

  // sort all columns of original table and store the sorted segments
  std::vector<SortColumnDefinition> sort_column_definitions;
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    const auto sort_column_definition = SortColumnDefinition{column_id};
    sort_column_definitions.emplace_back(sort_column_definition);
    column_data_distributions.emplace_back(original_table->get_column_data_distribution(column_id));

    // create a ChunkEncodingSpec for the sorted table
    const auto original_segment = table->get_chunk(ChunkID{0})->get_segment(column_id);
    const auto encoded_segment = std::dynamic_pointer_cast<AbstractEncodedSegment>(original_segment);
    const auto encoding_type = encoded_segment ? encoded_segment->encoding_type() : EncodingType::Unencoded;
    const auto segment_encoding_spec = SegmentEncodingSpec(encoding_type);
    chunk_encoding_spec.emplace_back(segment_encoding_spec);

    const auto sort_operator =
        std::make_shared<Sort>(table_wrapper, std::vector<SortColumnDefinition>{sort_column_definition},
                               table->target_chunk_size(), Sort::ForceMaterialization::Yes);
    sort_operator->execute();
    const auto sorted_table = std::const_pointer_cast<Table>(sort_operator->get_output());
    for (auto chunk_id = ChunkID{0}; chunk_id < sorted_table->chunk_count(); ++chunk_id) {
      const auto segment = sorted_table->get_chunk(chunk_id)->get_segment(column_id);
      if (!segments.contains(chunk_id)) segments[chunk_id] = Segments{};
      segments[chunk_id].emplace_back(segment);
    }
  }

  // build chunks from sorted segments
  std::vector<std::shared_ptr<Chunk>> chunks;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto mvcc_data = std::make_shared<MvccData>(segments[chunk_id].front()->size(), CommitID{0});
    const auto chunk = std::make_shared<Chunk>(segments[chunk_id], mvcc_data);
    chunk->finalize();
    chunk->set_individually_sorted_by(sort_column_definitions);
    chunks.emplace_back(chunk);
  }

  // build and encode sorted table
  const auto column_definitions = table->column_definitions();
  const auto all_sorted_table =
      std::make_shared<Table>(column_definitions, TableType::Data, std::move(chunks), UseMvcc::Yes);
  ChunkEncoder::encode_all_chunks(all_sorted_table, chunk_encoding_spec);

  return std::make_shared<const CalibrationTableWrapper>(
      CalibrationTableWrapper(all_sorted_table, sorted_table_name, column_data_distributions));
}

std::shared_ptr<const CalibrationTableWrapper> CalibrationTableGenerator::_generate_foreign_key_table(
    const std::shared_ptr<const CalibrationTableWrapper>& original_table,
    const std::shared_ptr<const SyntheticTableGenerator>& table_generator) const {
  const auto table_name = original_table->get_name() + "_unique";
  const auto& table = original_table->get_table();
  const auto table_size = table->row_count();
  std::vector<ColumnDataDistribution> data_distributions;
  std::vector<ColumnSpecification> column_specs;

  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    auto data_distribution = _column_data_distributions.at(column_id);
    data_distribution.max_value = table_size;
    data_distributions.emplace_back(data_distribution);

    const auto& original_column_spec = _column_specs.at(column_id);
    auto column_spec = ColumnSpecification(data_distribution, original_column_spec.data_type,
                                           original_column_spec.segment_encoding_spec, original_column_spec.name);
    column_specs.emplace_back(column_spec);
  }

  const auto new_table =
      table_generator->generate_table(column_specs, table->row_count(), table->target_chunk_size(), table->uses_mvcc());
  return std::make_shared<const CalibrationTableWrapper>(new_table, table_name, data_distributions);
}

}  // namespace opossum
