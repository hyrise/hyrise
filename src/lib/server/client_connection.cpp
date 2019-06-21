#include "client_connection.hpp"

#include <boost/asio.hpp>

#include "postgres_wire_handler.hpp"
#include "then_operator.hpp"
#include "use_boost_future.hpp"

namespace opossum {

using opossum::then_operator::then;

const auto ignore_sent_bytes = [](uint64_t sent_bytes) {};

ClientConnection::ClientConnection(boost::asio::ip::tcp::socket socket) : _socket(std::move(socket)) {
  _response_buffer.reserve(_max_response_size);
}

boost::future<uint32_t> ClientConnection::receive_startup_packet_header() {
  constexpr uint32_t STARTUP_HEADER_LENGTH = 8u;

  return _receive_bytes_async(STARTUP_HEADER_LENGTH) >> then >> PostgresWireHandler::handle_startup_package;
}

boost::future<void> ClientConnection::receive_startup_packet_body(uint32_t size) {
  return _receive_bytes_async(size) >> then >> [](InputPacket p) {
    // Read these values and ignore them
    PostgresWireHandler::handle_startup_package_content(p);
  };
}

boost::future<RequestHeader> ClientConnection::receive_packet_header() {
  constexpr uint32_t HEADER_LENGTH = 5u;

  return _receive_bytes_async(HEADER_LENGTH) >> then >> PostgresWireHandler::handle_header;
}

boost::future<std::string> ClientConnection::receive_simple_query_packet_body(uint32_t size) {
  return _receive_bytes_async(size) >> then >> PostgresWireHandler::handle_query_packet;
}

boost::future<ParsePacket> ClientConnection::receive_parse_packet_body(uint32_t size) {
  return _receive_bytes_async(size) >> then >> PostgresWireHandler::handle_parse_packet;
}

boost::future<BindPacket> ClientConnection::receive_bind_packet_body(uint32_t size) {
  return _receive_bytes_async(size) >> then >> PostgresWireHandler::handle_bind_packet;
}

boost::future<std::string> ClientConnection::receive_describe_packet_body(uint32_t size) {
  return _receive_bytes_async(size) >> then >> PostgresWireHandler::handle_describe_packet;
}

boost::future<void> ClientConnection::receive_sync_packet_body(uint32_t size) {
  // Packet has no content, we'll make the receive call anyways, just in case size > 0
  return _receive_bytes_async(size) >> then >> [](InputPacket packet) {};
}

boost::future<void> ClientConnection::receive_flush_packet_body(uint32_t size) {
  // Packet has no content, we'll make the receive call anyways, just in case size > 0
  return _receive_bytes_async(size) >> then >> [](InputPacket packet) {};
}

boost::future<std::string> ClientConnection::receive_execute_packet_body(uint32_t size) {
  return _receive_bytes_async(size) >> then >> PostgresWireHandler::handle_execute_packet;
}

boost::future<void> ClientConnection::send_ssl_denied() {
  // Don't use new_output_packet here, because this packet has special size requirements (only contains N, no size)
  auto output_packet = std::make_shared<OutputPacket>();
  PostgresWireHandler::write_value(*output_packet, NetworkMessageType::SslNo);

  return _send_bytes_async(output_packet, true) >> then >> ignore_sent_bytes;
}

boost::future<void> ClientConnection::send_auth() {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::AuthenticationRequest);
  PostgresWireHandler::write_value(*output_packet, htonl(0u));

  return _send_bytes_async(output_packet) >> then >> ignore_sent_bytes;
}

boost::future<void> ClientConnection::send_parameter_status(const std::string& key, const std::string& value) {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ParameterStatus);
  PostgresWireHandler::write_string(*output_packet, key);
  PostgresWireHandler::write_string(*output_packet, value);
  return _send_bytes_async(output_packet) >> then >> ignore_sent_bytes;
}

boost::future<void> ClientConnection::send_ready_for_query() {
  // ReadyForQuery packet 'Z' with transaction status Idle 'I'
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ReadyForQuery);
  PostgresWireHandler::write_value(*output_packet, TransactionStatusIndicator::Idle);

  return _send_bytes_async(output_packet, true) >> then >> ignore_sent_bytes;
}

boost::future<void> ClientConnection::send_error(const std::string& message) {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ErrorResponse);

  // Send the error message with type info 'M' that indicates that the following body is a plain message to be displayed
  PostgresWireHandler::write_value(*output_packet, NetworkMessageType::HumanReadableError);
  PostgresWireHandler::write_string(*output_packet, message);

  // Terminate the error response
  PostgresWireHandler::write_value(*output_packet, '\0');
  return _send_bytes_async(output_packet, true) >> then >> ignore_sent_bytes;
}

boost::future<void> ClientConnection::send_notice(const std::string& notice) {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::Notice);

  // Send notice message with type info 'M' that indicates that the following body is a plain message to be displayed
  PostgresWireHandler::write_value(*output_packet, NetworkMessageType::HumanReadableError);
  PostgresWireHandler::write_string(*output_packet, notice);

  // Terminate the notice response
  PostgresWireHandler::write_value(*output_packet, '\0');
  return _send_bytes_async(output_packet, true) >> then >> ignore_sent_bytes;
}

boost::future<void> ClientConnection::send_status_message(const NetworkMessageType& type) {
  auto output_packet = PostgresWireHandler::new_output_packet(type);
  return _send_bytes_async(output_packet) >> then >> ignore_sent_bytes;
}

boost::future<void> ClientConnection::send_row_description(const std::vector<ColumnDescription>& row_description) {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::RowDescription);

  // Int16 Specifies the number of fields in a row (can be zero).
  PostgresWireHandler::write_value(*output_packet, htons(static_cast<uint16_t>(row_description.size())));

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
    PostgresWireHandler::write_string(*output_packet, column_description.column_name);
    PostgresWireHandler::write_value(*output_packet, htonl(0u));  // no object id
    PostgresWireHandler::write_value(*output_packet, htons(0u));  // no attribute number

    PostgresWireHandler::write_value(*output_packet,
                                     htonl(static_cast<uint32_t>(column_description.object_id)));  // object id of type
    PostgresWireHandler::write_value(*output_packet,
                                     htons(static_cast<uint16_t>(column_description.type_width)));  // regular int
    PostgresWireHandler::write_value(*output_packet, htonl(-1));                                    // no modifier
    PostgresWireHandler::write_value(*output_packet, htons(0u));                                    // text format
  }

  return _send_bytes_async(output_packet) >> then >> ignore_sent_bytes;
}

boost::future<void> ClientConnection::send_data_row(const std::vector<std::string>& row_strings) {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::DataRow);

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

  // Number of columns in row
  PostgresWireHandler::write_value(*output_packet, htons(static_cast<uint16_t>(row_strings.size())));

  for (const auto& value_string : row_strings) {
    // Size of string representation of value, NOT of value type's size
    PostgresWireHandler::write_value(*output_packet, htonl(static_cast<uint32_t>(value_string.length())));

    // Text mode means that all values are sent as non-terminated strings
    PostgresWireHandler::write_string(*output_packet, value_string, false);
  }

  return _send_bytes_async(output_packet) >> then >> ignore_sent_bytes;
}

boost::future<void> ClientConnection::send_command_complete(const std::string& message) {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::CommandComplete);
  PostgresWireHandler::write_string(*output_packet, message);

  return _send_bytes_async(output_packet, true) >> then >> ignore_sent_bytes;
}

boost::future<InputPacket> ClientConnection::_receive_bytes_async(size_t size) {
  auto result = std::make_shared<InputPacket>();
  result->data.resize(size);

  // We need a copy of this client connection to outlive the async operation
  auto self = shared_from_this();
  return _socket.async_read_some(boost::asio::buffer(result->data, size), boost::asio::use_boost_future) >> then >>
         [self, result, size](uint64_t received_size) {
           // If this assertion should fail, we will end up in either the error handler for the current command or
           // the entire session. The connection may be closed but the server will keep running either way.
           Assert(received_size == size, "Client sent less data than expected.");

           result->offset = result->data.begin();
           return std::move(*result);
         };
}

boost::future<uint64_t> ClientConnection::_send_bytes_async(const std::shared_ptr<OutputPacket>& packet, bool flush) {
  const auto packet_size = packet->data.size();

  // If the packet is SslNo (size == 1), it has a special format and does not require a size
  if (packet_size > 1) {
    PostgresWireHandler::write_output_packet_size(*packet);
  }

  // If this fails, the connection may be closed but the server will keep running.
  Assert(packet_size <= _max_response_size,
         "The output packet is too big and cannot be sent. This should never happen!");

  if (_response_buffer.size() + packet_size > _max_response_size) {
    // We have to flush before we can actually process the data
    return _flush_async() >> then >> [this, packet, flush](uint64_t) { return _send_bytes_async(packet, flush); };
  }

  _response_buffer.insert(_response_buffer.end(), packet->data.begin(), packet->data.end());

  if (flush) {
    return _flush_async() >> then >> [=](uint64_t) { return static_cast<uint64_t>(packet_size); };
  } else {
    // Return an already resolved future (we have just written data to the buffer)
    return boost::make_ready_future<uint64_t>(packet_size);
  }
}

boost::future<uint64_t> ClientConnection::_flush_async() {
  return _socket.async_send(boost::asio::buffer(_response_buffer), boost::asio::use_boost_future) >> then >>
         [this](uint64_t sent_bytes) {
           // If this fails, the connection may be closed but the server will keep running.
           Assert(sent_bytes == _response_buffer.size(), "Could not send all data");
           _response_buffer.clear();
           return static_cast<uint64_t>(sent_bytes);
         };
}
}  // namespace opossum
