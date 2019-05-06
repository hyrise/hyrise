#include "postgres_handler.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>

#include "network_message_types.hpp"
#include "utils/assert.hpp"

namespace opossum {

PostgresHandler::PostgresHandler(boost::asio::ip::tcp::socket socket) :
  _read_buffer(std::move(socket)),
  _write_buffer(std::move(socket)) {}

uint32_t PostgresHandler::read_startup_packet() {
  constexpr auto ssl_request_code = 80877103u;

  auto startup_packet_length = ntohl(_read_buffer.get_value<uint32_t>());
  auto protocol_version = ntohl(_read_buffer.get_value<uint32_t>());

  // We currently do not support SSL
  if (protocol_version == ssl_request_code) {
    ssl_deny();
    return read_startup_packet();
  } else {
    // Substract uint32_t twice, since both packet length and protocol version have been read already
    return startup_packet_length - 2 * sizeof(uint32_t);
  }
}

NetworkMessageType PostgresHandler::get_packet_type() {
  return _read_buffer.get_message_type();
}

const std::string PostgresHandler::read_packet_body() {
  // TODO(toni): refactor
  auto query_length = ntohl(_read_buffer.get_value<uint32_t>()) - 4u;
  return _read_buffer.get_string(query_length);
}

void PostgresHandler::ssl_deny() {
  // The SSL deny packet has a special format. It does not have a field indicating the packet size.
  _write_buffer.put_value(NetworkMessageType::SslNo);
  _write_buffer.flush();
}

void PostgresHandler::handle_startup_packet_body(const uint32_t size) {
  // As of now, we don't do anything with the startup packet body. It contains authentication data and the database
  // name the user desires to connect to.
  _read_buffer.reset();
}

void PostgresHandler::send_authentication() {
  // TODO(toni): refactoring here, too many magic numbers
  _write_buffer.put_value(NetworkMessageType::AuthenticationRequest);
  _write_buffer.put_value(htonl(8u));
  _write_buffer.put_value(htonl(0u));
}

void PostgresHandler::send_parameter(const std::string& key, const std::string& value) {
  auto body_length = sizeof(uint32_t) + key.length() + 1u + value.length() + 1u;
  _write_buffer.put_value(NetworkMessageType::ParameterStatus);
  _write_buffer.put_value(htonl(body_length));
  _write_buffer.put_string(key);
  _write_buffer.put_string(value);
}

void PostgresHandler::send_ready_for_query() {
  _write_buffer.put_value(NetworkMessageType::ReadyForQuery);
  _write_buffer.put_value(htonl(sizeof(uint32_t) + sizeof(TransactionStatusIndicator::Idle)));
  _write_buffer.put_value(TransactionStatusIndicator::Idle);
  _write_buffer.flush();
}

void PostgresHandler::command_complete(const std::string& command_complete_message) {
  auto packet_size = sizeof(uint32_t) + command_complete_message.length() + sizeof(char);

  _write_buffer.put_value(NetworkMessageType::CommandComplete);
  _write_buffer.put_value(htonl(packet_size));
  _write_buffer.put_string(command_complete_message);
}

void PostgresHandler::send_row_description(const std::vector<RowDescription>& row_description) {
  _write_buffer.put_value(NetworkMessageType::RowDescription);

  auto packet_size = sizeof(uint32_t) + sizeof(htons(row_description.size()));

  for (const auto& single_row_description : row_description) {
    packet_size = packet_size + sizeof('\0') + 3 * sizeof(uint32_t) + 3 * sizeof(uint16_t) +
                  single_row_description.column_name.size();
  }

  _write_buffer.put_value(htonl(packet_size));

  // Int16 Specifies the number of fields in a row (can be zero).
  _write_buffer.put_value(htons(row_description.size()));

  /* FROM: https://www.postgresql.org/docs/current/static/protocol-message-formats.html
   *
   * Then, for each field, there is the following:
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

  for (const auto& column_description : row_description) {
    _write_buffer.put_string(column_description.column_name);
    _write_buffer.put_value(htonl(0u));                             // no object id
    _write_buffer.put_value(htons(0u));                             // no attribute number
    _write_buffer.put_value(htonl(column_description.object_id));   // object id of type
    _write_buffer.put_value(htons(column_description.type_width));  // regular int
    _write_buffer.put_value(htonl(-1));                             // no modifier
    _write_buffer.put_value(htons(0u));                             // text format
  }
}

void PostgresHandler::send_data_row(const std::vector<std::string>& row_strings) {
  _write_buffer.put_value(NetworkMessageType::DataRow);

  auto packet_size = sizeof(uint32_t) + sizeof(uint16_t);

  for (const auto& attribute : row_strings) {
    packet_size = packet_size + attribute.size() + sizeof(uint32_t);
  }

  _write_buffer.put_value(htonl(packet_size));

  // Number of columns in row
  _write_buffer.put_value(htons(row_strings.size()));

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
    _write_buffer.put_value(htonl(value_string.length()));

    // Text mode means that all values are sent as non-terminated strings
    _write_buffer.put_string(value_string, false);
  }
}
}  // namespace opossum
