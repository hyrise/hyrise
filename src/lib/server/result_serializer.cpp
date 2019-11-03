#include "result_serializer.hpp"
#include "lossy_cast.hpp"

namespace opossum {

template <typename SocketType>
void ResultSerializer::send_table_description(
    const std::shared_ptr<const Table>& table,
    const std::shared_ptr<PostgresProtocolHandler<SocketType>>& postgres_protocol_handler) {
  // Calculate sum of length of all column names
  uint32_t column_name_length_sum = 0;
  for (auto& column_name : table->column_names()) {
    column_name_length_sum += column_name.size();
  }

  postgres_protocol_handler->send_row_description_header(column_name_length_sum,
                                                         static_cast<uint16_t>(table->column_count()));

  for (ColumnID column_id{0u}; column_id < table->column_count(); ++column_id) {
    uint32_t object_id = 0;
    int16_t type_width = 0;

    // Documentation of the PostgreSQL object_ids can be found at:
    // https://crate.io/docs/crate/reference/en/latest/interfaces/postgres.html
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
      case DataType::Null:
        Fail("Bad DataType");
    }
    postgres_protocol_handler->send_row_description(table->column_name(column_id), object_id, type_width);
  }
}

template <typename SocketType>
void ResultSerializer::send_query_response(
    const std::shared_ptr<const Table>& table,
    const std::shared_ptr<PostgresProtocolHandler<SocketType>>& postgres_protocol_handler) {
  auto values_as_strings = std::vector<std::optional<std::string>>(table->column_count());

  const auto chunk_count = table->chunk_count();

  // Iterate over each chunk in result table
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; chunk_id++) {
    const auto& current_chunk = table->get_chunk(chunk_id);
    const auto chunk_size = current_chunk->size();
    const auto& segments = current_chunk->segments();
    // Iterate over each row in chunk
    for (ChunkOffset current_chunk_offset{0}; current_chunk_offset < chunk_size; ++current_chunk_offset) {
      auto string_length_sum = 0;
      // Iterate over each attribute in row
      for (size_t current_segment = 0; current_segment < segments.size(); current_segment++) {
        const auto attribute_value = (*segments[current_segment])[current_chunk_offset];
        // The PostgreSQL protocol requires the conversion of values to strings
        const auto string_value = lossy_variant_cast<pmr_string>(attribute_value);
        if (string_value.has_value()) {
          // Sum up string lengths for a row to save an extra loop during serialization
          string_length_sum += string_value.value().size();
        }
        values_as_strings[current_segment] = string_value;
      }
      postgres_protocol_handler->send_data_row(values_as_strings, string_length_sum);
    }
  }
}

std::string ResultSerializer::build_command_complete_message(const OperatorType root_operator_type,
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

template void ResultSerializer::send_table_description<Socket>(const std::shared_ptr<const Table>&,
                                                               const std::shared_ptr<PostgresProtocolHandler<Socket>>&);

template void ResultSerializer::send_table_description<boost::asio::posix::stream_descriptor>(
    const std::shared_ptr<const Table>&,
    const std::shared_ptr<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>>&);

template void ResultSerializer::send_query_response<Socket>(const std::shared_ptr<const Table>&,
                                                            const std::shared_ptr<PostgresProtocolHandler<Socket>>&);

template void ResultSerializer::send_query_response<boost::asio::posix::stream_descriptor>(
    const std::shared_ptr<const Table>&,
    const std::shared_ptr<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>>&);

}  // namespace opossum
