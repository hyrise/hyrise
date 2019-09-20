#include "response_builder.hpp"
#include "lossy_cast.hpp"

namespace opossum {

void ResponseBuilder::build_and_send_row_description(
    std::shared_ptr<const Table> table, const std::shared_ptr<PostgresProtocolHandler>& postgres_protocol_handler) {
  // Calculate sum of length of all column names
  uint32_t column_lengths_total = 0;
  for (auto& column_name : table->column_names()) {
    column_lengths_total += column_name.size();
  }

  postgres_protocol_handler->set_row_description_header(column_lengths_total, table->column_count());

  for (ColumnID column_id{0u}; column_id < table->column_count(); ++column_id) {
    uint32_t object_id;
    int16_t type_width;

    switch (table->column_data_type(column_id)) {
      case DataType::Int:
        object_id = 23;
        type_width = 4;
        break;
      case DataType::Long:
        object_id = 20;
        type_width = 8;
        break;
      case DataType::Float:
        object_id = 700;
        type_width = 4;
        break;
      case DataType::Double:
        object_id = 701;
        type_width = 8;
        break;
      case DataType::String:
        object_id = 25;
        type_width = -1;
        break;
      default:
        Fail("Bad DataType");
    }
    postgres_protocol_handler->send_row_description(table->column_name(column_id), object_id, type_width);
  }
}

uint64_t ResponseBuilder::build_and_send_query_response(
    std::shared_ptr<const Table> table, const std::shared_ptr<PostgresProtocolHandler>& postgres_protocol_handler) {
  auto attribute_strings = std::vector<std::string>(table->column_count());
  const auto chunk_count = table->chunk_count();

  // Iterate over each chunk in result table
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; chunk_id++) {
    const auto chunk_size = table->get_chunk(chunk_id)->size();
    const auto segments = table->get_chunk(chunk_id)->segments();
    // Iterate over each row in chunk
    for (ChunkOffset current_chunk_offset{0}; current_chunk_offset < chunk_size; ++current_chunk_offset) {
      auto string_lengths = 0;
      // Iterate over each attribute in row
      for (size_t current_segment = 0; current_segment < segments.size(); current_segment++) {
        const auto attribute_value = segments[current_segment]->operator[](current_chunk_offset);
        const auto string_value = lossy_variant_cast<pmr_string>(attribute_value).value();
        // Sum up string lengths for a row to save an extra loop during serialization
        string_lengths += string_value.size();
        attribute_strings[current_segment] = std::move(string_value);
      }
      postgres_protocol_handler->send_data_row(attribute_strings, string_lengths);
    }
  }
  return table->row_count();
}

std::string ResponseBuilder::build_command_complete_message(const OperatorType root_operator_type,
                                                            const uint64_t row_count) {
  switch (root_operator_type) {
    case OperatorType::Insert: {
      // 0 is ignored OID and 1 inserted row
      return "INSERT 0 1";
    }
    case OperatorType::Update: {
      // We do not return how many rows are affected, because we don't track this
      // information
      return "UPDATE -1";
    }
    case OperatorType::Delete: {
      // We do not return how many rows are affected, because we don't track this
      // information
      return "DELETE -1";
    }
    default:
      // Assuming normal query
      return "SELECT " + std::to_string(row_count);
  }
}

}  // namespace opossum
