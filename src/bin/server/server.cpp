// Files in the /bin folder are not tested. Everything that can be tested should be in the /lib folder and this file
// should be as short as possible.

#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>

#include <boost/asio.hpp>
#include "postgres_wire_handler.hpp"

namespace opossum {

using boost::asio::ip::tcp;

static const uint32_t HEADER_LENGTH = 5u;
static const uint32_t STARTUP_HEADER_LENGTH = 8u;

class Session : public std::enable_shared_from_this<Session> {
 public:
  explicit Session(tcp::socket socket) : _socket(std::move(socket)) {}

  void start() { read_startup_packet(); }

 private:
  void read_startup_packet() {
    _input_packet.offset = _input_packet.data.begin();

    // boost::asio::read will read until either the buffer is full or STARTUP_HEADER_LENGTH bytes are read from socket
    boost::asio::read(_socket, boost::asio::buffer(_input_packet.data, STARTUP_HEADER_LENGTH));

    // Content length tells us how much more data there is in this packet
    auto content_length = _pg_handler.handle_startup_package(_input_packet);

    // If we have no further content, this is a special 8 byte SSL packet,
    // which we decline with 'N' because we don't support SSL
    if (content_length == 0) {
      _output_packet.data.clear();
      _pg_handler.write_value(_output_packet, 'N');
      auto ssl_yes = boost::asio::buffer(_output_packet.data);
      boost::asio::write(_socket, ssl_yes);

      // Wait for actual startup packet
      return read_startup_packet();
    }

    auto bytes_read = boost::asio::read(_socket, boost::asio::buffer(_input_packet.data, content_length));
    // TODO: not sure if this can happen, and not sure how to deal with it yet
    if (bytes_read != content_length) {
      std::cout << "Bad read. Got: " << bytes_read << " bytes, but expected: " << content_length << std::endl;
    }
    _pg_handler.handle_startup_package_content(_input_packet, bytes_read);

    // We still need to reset the ByteBuffer manually, so the next reader can start of at the correct position.
    // This should be hidden later on
    _input_packet.offset = _input_packet.data.begin();
    send_auth();
  }

  void send_auth() {
    // This packet is our AuthenticationOK, which means we do not require any auth.
    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'R');
    _pg_handler.write_value(_output_packet, htonl(8u));
    _pg_handler.write_value(_output_packet, htonl(0u));
    auto auth = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, auth);

    // This is a Status Parameter which is useless but it seems like clients expect at least one. Contains dummy value.
    //    _output_packet.data.clear();
    //    _pg_handler.write_value(_output_packet, 'S');
    //    _pg_handler.write_value<uint32_t>(_output_packet, htonl(28u));
    //    _pg_handler.write_string(_output_packet, "client_encoding");
    //    _pg_handler.write_string(_output_packet, "UNICODE");
    //    auto params = boost::asio::buffer(_output_packet.data);
    //    boost::asio::write(_socket, params);

    send_ready_for_query();
  }

  void send_ready_for_query() {
    // ReadyForQuery packet 'Z' with transaction status Idle 'I'
    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'Z');
    _pg_handler.write_value(_output_packet, htonl(5u));
    _pg_handler.write_value(_output_packet, 'I');
    auto ready = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, ready);

    read_query();
  }

  void read_query() {
    boost::asio::read(_socket, boost::asio::buffer(_input_packet.data, HEADER_LENGTH));
    auto content_length = _pg_handler.handle_header(_input_packet);

    boost::asio::read(_socket, boost::asio::buffer(_input_packet.data, content_length));
    auto sql = _pg_handler.handle_query_packet(_input_packet, content_length);

    send_row_description(sql);
  }

  void send_row_description(const std::string& sql) {
    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'T');

    auto total_bytes = sizeof(uint32_t) + 6 + 3 * sizeof(uint32_t) + 4 * sizeof(uint16_t);
    _pg_handler.write_value(_output_packet, htonl(total_bytes));

    // Int16 Specifies the number of fields in a row (can be zero).
    _pg_handler.write_value(_output_packet, htons(1u));

    /* FROM: https://www.postgresql.org/docs/current/static/protocol-message-formats.html
     *
     * Then, for each field, there is the following:
    String
    The field name.

    Int32 - If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.

    Int16 - If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.

    Int32 - The object ID of the field's data type.
            Found at: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h

    Int16 - The data type size (see pg_type.typlen). Note that negative values denote variable-width types.

    Int32 - The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.

    Int16 - The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
    */
    _pg_handler.write_string(_output_packet, "foo_a");   // 6 bytes (null terminated)
    _pg_handler.write_value(_output_packet, htonl(0u));  // no object id
    _pg_handler.write_value(_output_packet, htons(0u));  // no attribute number

    _pg_handler.write_value(_output_packet, htonl(23));  // object id of int32

    _pg_handler.write_value(_output_packet, htons(sizeof(uint32_t)));  // regular int
    _pg_handler.write_value(_output_packet, htonl(-1));                // no modifier
    _pg_handler.write_value(_output_packet, htons(0u));                // text format

    auto row_description = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, row_description);

    send_row_data();
  }

  void send_row_data() {
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
      The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.

      Byten
      The value of the column, in the format indicated by the associated format code. n is the above length.
     */

    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'D');

    auto total_bytes = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint32_t) + 3;
    _pg_handler.write_value(_output_packet, htonl(total_bytes));

    // 1 column
    _pg_handler.write_value(_output_packet, htons(1u));

    // Size of string representation of value, NOT of value type's size
    _pg_handler.write_value(_output_packet, htonl(3u));

    // Text mode means that all values are sent as non-terminated strings
    _pg_handler.write_string(_output_packet, "100", false);

    auto data_row = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, data_row);

    // CommandComplete
    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'C');

    total_bytes = sizeof(uint32_t) + 9;
    _pg_handler.write_value(_output_packet, htonl(total_bytes));

    // Completed SELECT statement with 1 result row
    _pg_handler.write_string(_output_packet, "SELECT 1");

    auto command_complete = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, command_complete);

    // For now, we just go back to reading the next query,
    // This not not work yet, as we are reading wrong bytes or the client terminates which we don't handle
    send_ready_for_query();
  }

  tcp::socket _socket;
  PostgresWireHandler _pg_handler;
  InputPacket _input_packet;
  OutputPacket _output_packet;
};

class server {
 public:
  server(boost::asio::io_service& io_service, unsigned short port)
      : acceptor_(io_service, tcp::endpoint(tcp::v4(), port)), socket_(io_service) {
    do_accept();
  }

 private:
  void do_accept() {
    auto start_session = [this](boost::system::error_code ec) {
      if (!ec) {
        std::make_shared<Session>(std::move(socket_))->start();
      }
      do_accept();
    };

    acceptor_.async_accept(socket_, start_session);
  }

  tcp::acceptor acceptor_;
  tcp::socket socket_;
};

}  // namespace opossum

int main(int argc, char* argv[]) {
  try {
    if (argc != 2) {
      std::cerr << "Usage: async_tcp_echo_server <port>\n";
      return 1;
    }

    boost::asio::io_service io_service;

    opossum::server s(io_service, std::atoi(argv[1]));

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
