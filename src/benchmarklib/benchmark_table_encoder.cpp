#include "benchmark_table_encoder.hpp"

#include "constant_mappings.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "encoding_config.hpp"

namespace {

using namespace opossum;  // NOLINT

ChunkEncodingSpec _generate_chunk_encoding_spec(const Chunk& chunk) {
  auto chunk_encoding_spec = ChunkEncodingSpec{chunk.column_count()};

  for (auto column_id = ColumnID{0}; column_id < chunk.column_count(); ++column_id) {

  }
}

}

namespace opossum {

bool BenchmarkTableEncoder::encode(const std::string& table_name, const std::shared_ptr<Table>& table,
                                   const EncodingConfig& encoding_config, std::ostream& out) {
  const auto& type_mapping = encoding_config.type_encoding_mapping;
  const auto& custom_mapping = encoding_config.custom_encoding_mapping;

  const auto& column_mapping_it = custom_mapping.find(table_name);
  const auto table_has_custom_encoding = column_mapping_it != custom_mapping.end();

  ChunkEncodingSpec chunk_encoding_spec;

  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    // Check if a column specific encoding was specified
    if (table_has_custom_encoding) {
      const auto& column_name = table->column_name(column_id);
      const auto& encoding_by_column_name = column_mapping_it->second;
      const auto& segment_encoding = encoding_by_column_name.find(column_name);
      if (segment_encoding != encoding_by_column_name.end()) {
        // The column type has a custom encoding
        chunk_encoding_spec.push_back(segment_encoding->second);
        continue;
      }
    }

    // Check if a type specific encoding was specified
    const auto& column_data_type = table->column_data_type(column_id);
    const auto& encoding_by_data_type = type_mapping.find(column_data_type);
    if (encoding_by_data_type != type_mapping.end()) {
      // The column type has a specific encoding
      chunk_encoding_spec.push_back(encoding_by_data_type->second);
      continue;
    }

    // No column-specific or type-specific encoding was specified.
    // Use default if it is compatible with the column type or leave column Unencoded if it is not.
    if (encoding_supports_data_type(encoding_config.default_encoding_spec.encoding_type, column_data_type)) {
      chunk_encoding_spec.push_back(encoding_config.default_encoding_spec);
    } else {
      out << " - Column '" << table_name << "." << table->column_name(column_id) << "' of type ";
      out << data_type_to_string.left.at(column_data_type) << " cannot be encoded as ";
      out << encoding_type_to_string.left.at(encoding_config.default_encoding_spec.encoding_type) << " and is ";
      out << "left Unencoded." << std::endl;
      chunk_encoding_spec.push_back(EncodingType::Unencoded);
    }
  }

  /**
   * Encode chunks
   */
  auto encoding_performed = false;
  const auto column_data_types = table->column_data_types();

  for (const auto& chunk : table->chunks()) {
    if (chunk_encoding_spec != _generate_chunk_encoding_spec(*chunk)) {
      ChunkEncoder::encode_chunk(chunk, column_data_types, chunk_encoding_spec);
      encoding_performed = true;
    }
  }

  return encoding_performed;
}

}  // namespace opossum
