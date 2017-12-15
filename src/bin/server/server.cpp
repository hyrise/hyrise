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
      _pg_handler.write_value(_output_packet, 0u);
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
    // This packet is our AuthenticationOK, which means we do not require any auth. However, psql does not accept this.
    // TODO: figure out how to send correct AuthenticationOK (wireshark says this is valid)
    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'R');
    _pg_handler.write_value(_output_packet, htonl(8u));
    _pg_handler.write_value(_output_packet, htonl(0u));
    auto auth = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, auth);

    // This is a Status Parameter which is useless but it seems like clients expect at least one. Contains dummy value.
    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'S');
    _pg_handler.write_value<uint32_t>(_output_packet, htonl(28u));
    _pg_handler.write_string(_output_packet, "client_encoding");
    _pg_handler.write_string(_output_packet, "UNICODE");
    auto params = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, params);

    send_ready_for_query();
  }

  void send_ready_for_query() {
    // ReadyForQuery packet 'Z' with status Idle 'I'
    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'Z');
    _pg_handler.write_value(_output_packet, htonl(5u));
    _pg_handler.write_value(_output_packet, 'I');
    auto ready = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, ready);

    read_query();
  }

  void read_query() {
    boost::system::error_code ec;

    boost::asio::read(_socket, boost::asio::buffer(_input_packet.data, HEADER_LENGTH));
    auto content_length = _pg_handler.handle_header(_input_packet);

    boost::asio::read(_socket, boost::asio::buffer(_input_packet.data, content_length));
    auto sql = _pg_handler.handle_query_packet(_input_packet, content_length);

    send_row_description(sql);
  }

  void send_row_description(const std::string& sql) {
    _output_packet.data.clear();
    // TODO: find out correct format once we get here
    _pg_handler.write_string(_output_packet, sql);
    auto query = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, query);

    // For now, we just go back to reading the next query
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
