#include "postgres_protocol_handler.hpp"

#include "network_message_types.hpp"

namespace opossum {

static constexpr auto LENGTH_FIELD_SIZE = 4u;

PostgresProtocolHandler::PostgresProtocolHandler(const std::shared_ptr<Socket> socket) : _read_buffer(socket), _write_buffer(socket) {}

uint32_t PostgresProtocolHandler::read_startup_packet() {
  constexpr auto ssl_request_code = 80877103u;

  const auto body_length = _read_buffer.get_value<uint32_t>();
  const auto protocol_version = _read_buffer.get_value<uint32_t>();

  // We currently do not support SSL
  if (protocol_version == ssl_request_code) {
    _ssl_deny();
    return read_startup_packet();
  } else {
    // Substract uint32_t twice, since both packet length and protocol version have been read already
    return body_length - 2 * LENGTH_FIELD_SIZE;
  }
}

NetworkMessageType PostgresProtocolHandler::get_packet_type() { return _read_buffer.get_message_type(); }

std::string PostgresProtocolHandler::read_query_packet() {
  const auto query_length = _read_buffer.get_value<uint32_t>() - LENGTH_FIELD_SIZE;
  return _read_buffer.get_string(query_length);
}

void PostgresProtocolHandler::_ssl_deny() {
  // The SSL deny packet has a special format. It does not have a field indicating the packet size.
  _write_buffer.put_value(NetworkMessageType::SslNo);
  _write_buffer.flush();
}

void PostgresProtocolHandler::handle_startup_packet_body(const uint32_t size) {
  // As of now, we don't do anything with the startup packet body. It contains authentication data and the
  // database name the user desires to connect to. Hence, we read this information from the network device as
  // one large string and throw it away.
  _read_buffer.get_string(size, false);
}

void PostgresProtocolHandler::send_authentication() {
  _write_buffer.put_value(NetworkMessageType::AuthenticationRequest);
  // Since we dont have any authentication mechanism, authentication is always successful
  constexpr uint32_t authentication_successful = 0;
  _write_buffer.put_value<uint32_t>(sizeof(LENGTH_FIELD_SIZE) + sizeof(authentication_successful));
  _write_buffer.put_value<uint32_t>(authentication_successful);
}

void PostgresProtocolHandler::send_parameter(const std::string& key, const std::string& value) {
  const auto body_length = sizeof(LENGTH_FIELD_SIZE) + key.size() + value.size() + 2u /* null terminator */; 
  _write_buffer.put_value(NetworkMessageType::ParameterStatus);
  _write_buffer.put_value<uint32_t>(body_length);
  _write_buffer.put_string(key);
  _write_buffer.put_string(value);
}

void PostgresProtocolHandler::send_ready_for_query() {
  _write_buffer.put_value(NetworkMessageType::ReadyForQuery);
  _write_buffer.put_value<uint32_t>(sizeof(LENGTH_FIELD_SIZE) + sizeof(TransactionStatusIndicator::Idle));
  _write_buffer.put_value(TransactionStatusIndicator::Idle);
  _write_buffer.flush();
}

void PostgresProtocolHandler::command_complete(const std::string& command_complete_message) {
  const auto packet_size = sizeof(LENGTH_FIELD_SIZE) + command_complete_message.size() + 1u /* null terminator */;
  _write_buffer.put_value(NetworkMessageType::CommandComplete);
  _write_buffer.put_value<uint32_t>(packet_size);
  _write_buffer.put_string(command_complete_message);
}

void PostgresProtocolHandler::set_row_description_header(const uint32_t total_column_name_length, const uint16_t column_count) {
  _write_buffer.put_value(NetworkMessageType::RowDescription);

  /* Each column has the following fields within the message:
   FROM: https://www.postgresql.org/docs/current/static/protocol-message-formats.html

   String
   The field name.

   Int32
   If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.

   Int16
   If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.

   Int32
   The object ID of the field's data type.
   Found at: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h

   Int16
   The data type size (see pg_type.typlen). Note that negative values denote variable-width types.

   Int32
   The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.

   Int16
   The format code being used for the field. Currently will be zero (text) or one (binary).
   In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always
   be zero.
   */

  // Total message length can be calculated this way:

  // length field + column count + values for each column
  const auto packet_size = sizeof(uint32_t) + sizeof(uint16_t) +
                           column_count * (sizeof('\0') + 3 * sizeof(uint32_t) + 3 * sizeof(uint16_t)) +
                           total_column_name_length;
  _write_buffer.put_value<uint32_t>(packet_size);
  // Specifies the number of fields in a row (can be zero).
  _write_buffer.put_value<uint16_t>(column_count);
}

void PostgresProtocolHandler::send_row_description(const std::string& column_name, const uint32_t object_id,
                                           const int16_t type_width) {
  _write_buffer.put_string(column_name);          // Column name
  _write_buffer.put_value<int32_t>(0u);           // No object id
  _write_buffer.put_value<int16_t>(0u);           // No attribute number
  _write_buffer.put_value<int32_t>(object_id);    // Object id of type
  _write_buffer.put_value<int16_t>(type_width);   // Data type size
  _write_buffer.put_value<int32_t>(-1);           // No modifier
  _write_buffer.put_value<int16_t>(0u);           // Text format
}

void PostgresProtocolHandler::send_data_row(const std::vector<std::string>& row_strings) {
  // TODO(toni): refac here
  _write_buffer.put_value(NetworkMessageType::DataRow);

  auto packet_size = sizeof(uint32_t) + sizeof(uint16_t);

  for (const auto& attribute : row_strings) {
    packet_size = packet_size + attribute.size() + sizeof(uint32_t);
  }

  _write_buffer.put_value<uint32_t>(packet_size);

  // Number of columns in row
  _write_buffer.put_value<uint16_t>(row_strings.size());

  /*
  DataRow (B)
  Byte1('D')
  Identifies the message as a data row.

  Int32
  Length of message contents in bytes, including self.

  Int16
  The number of column values that follow (possibly zero).

  Next, the following pair of fields appear for each column:

  Int32
  The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case,
  -1 indicates a NULL column value. No value bytes follow in the NULL case.

  Byte n
  The value of the column, in the format indicated by the associated format code. n is the above length.
  */

  for (const auto& value_string : row_strings) {
    // Size of string representation of value, NOT of value type's size
    _write_buffer.put_value<uint32_t>(value_string.size());

    // Text mode means all values are sent as non-terminated strings
    _write_buffer.put_string(value_string, false);
  }
}

std::pair<std::string, std::string> PostgresProtocolHandler::read_parse_packet() {
    // TODO(toni): refac here
  _read_buffer.get_value<uint32_t>();  // Ignore packet size

  const std::string statement_name = _read_buffer.get_string();
  const std::string query = _read_buffer.get_string();
  const auto network_parameter_data_types = _read_buffer.get_value<uint16_t>();

  for (auto i = 0; i < network_parameter_data_types; i++) {
    /*auto parameter_data_types = */ _read_buffer.get_value<uint32_t>();
  }

  return {std::move(statement_name), std::move(query)};
}

void PostgresProtocolHandler::read_sync_packet() {
  // This packet has no body. Hence, only read and ignore its size.
  _read_buffer.get_value<uint32_t>();
}

PreparedStatementDetails PostgresProtocolHandler::read_bind_packet() {
  _read_buffer.get_value<uint32_t>();
  auto portal = _read_buffer.get_string();
  auto statement_name = _read_buffer.get_string();
  auto num_format_codes = _read_buffer.get_value<int16_t>();

  // auto format_codes = std::vector<int16_t>();
  // format_codes.reserve(num_format_codes);

  for (auto i = 0; i < num_format_codes; i++) {
    // format_codes.emplace_back(_read_buffer.get_value<int16_t>());
    _read_buffer.get_value<int16_t>();
  }

  auto num_parameter_values = _read_buffer.get_value<int16_t>();

  // TODO(toni): room for refactoring here?
  std::vector<AllTypeVariant> parameter_values;
  for (auto i = 0; i < num_parameter_values; ++i) {
    // TODO(toni): include null terminator
    const auto parameter_value_length = _read_buffer.get_value<int32_t>();
    const std::string x = _read_buffer.get_string(parameter_value_length, false);
    // auto x = _read_buffer.get_string();
    // const pmr_string x_str(x.begin(), x.end());
    parameter_values.emplace_back(x.c_str());
  }

  auto num_result_column_format_codes = _read_buffer.get_value<int16_t>();

  for (auto i = 0; i < num_result_column_format_codes; i++) {
    // format_codes.emplace_back(_read_buffer.get_value<int16_t>());
    _read_buffer.get_value<int16_t>();
  }
  // auto result_column_format_codes = read_values<int16_t>(num_result_column_format_codes);

  return {statement_name, portal, std::move(parameter_values)};
}

void PostgresProtocolHandler::read_describe_packet() {
  const auto packet_length = _read_buffer.get_value<uint32_t>();
  const auto object_to_describe = _read_buffer.get_value<char>();
  const auto statement_or_portal_name = _read_buffer.get_string(packet_length - sizeof(uint32_t) - sizeof(char));

  // statement descriptions are returned as two separate messages:
  // ParameterDescription and RowDescription
  // portal descriptions are just RowDescriptions
  if (object_to_describe == 'S') {
    // TODO(toni): describe portal
  }
}

std::string PostgresProtocolHandler::read_execute_packet() {
  const auto packet_length = _read_buffer.get_value<uint32_t>();
  const auto portal = _read_buffer.get_string(packet_length - 2 * sizeof(uint32_t));
  // TODO(toni): necessary?
  /*const auto max_rows = */ _read_buffer.get_value<int32_t>();
  return portal;
}

void PostgresProtocolHandler::send_status_message(const NetworkMessageType message_type) {
  _write_buffer.put_value(message_type);
  _write_buffer.put_value<uint32_t>(LENGTH_FIELD_SIZE);
}

}  // namespace opossum
