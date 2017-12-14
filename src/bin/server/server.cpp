// Files in the /bin folder are not tested. Everything that can be tested should be in the /lib folder and this file
// should be as short as possible.

#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>

#include <boost/asio.hpp>
#include "postgres_wire_handler.hpp"

namespace {
uint32_t uint32_endian_swap(uint32_t num) {
  return ((num & 0xFF000000) >> 24) | ((num & 0x00FF0000) >> 8 ) | ((num & 0x0000FF00) << 8 ) | (num << 24);
}
}


namespace opossum {

using boost::asio::ip::tcp;

class Session : public std::enable_shared_from_this<Session> {
 public:
  explicit Session(tcp::socket socket) : _socket(std::move(socket)) {}

  void start() {
    read_startup_packet();
  }

 private:
  void read_startup_packet() {
    _input_packet.offset = _input_packet.data.begin();

    const auto startup_header_length = 8u;
    boost::asio::read(_socket, boost::asio::buffer(_input_packet.data, startup_header_length));

    auto content_length = _pg_handler.handle_startup_package(_input_packet);
    if (content_length == 0) {
      // Special SSL packet
      _output_packet.data.clear();
      _pg_handler.write_value(_output_packet, 'N');
      _pg_handler.write_value(_output_packet, 0u);
      auto ssl_yes = boost::asio::buffer(_output_packet.data);
      boost::asio::write(_socket, ssl_yes);

      // Wait for actual startup packet
      return read_startup_packet();
    }

    auto bytes_read = boost::asio::read(_socket, boost::asio::buffer(_input_packet.data, content_length));
    if (bytes_read != content_length) {
      std::cout << "Bad read. Got: " << bytes_read << " but expected: " << content_length << std::endl;
    }
    _pg_handler.handle_startup_package_content(_input_packet, bytes_read);
    _input_packet.offset = _input_packet.data.begin();
    send_auth();
  }

  void send_auth() {
    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'R');
    _pg_handler.write_value<uint32_t>(_output_packet, uint32_endian_swap(8u));
    _pg_handler.write_value<uint32_t>(_output_packet, uint32_endian_swap(0u));
    auto auth = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, auth);

    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'S');
    _pg_handler.write_value<uint32_t>(_output_packet, uint32_endian_swap(22u));
    _pg_handler.write_string(_output_packet, "server_version");
    _pg_handler.write_string(_output_packet, "0.1");
    auto params = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, params);

    send_ready_for_query();
  }

  void send_ready_for_query() {
//    auto self(shared_from_this());
//
//    auto query_read = [this, self](boost::system::error_code ec, size_t length) {
//      if (!ec) {
//        read_query();
//      }
//    };

    _output_packet.data.clear();
    _pg_handler.write_value(_output_packet, 'Z');
    _pg_handler.write_value(_output_packet, 'I');
    auto ready = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, ready);

    read_query();
  }

  void read_query() {
//    auto self(shared_from_this());
//
//    auto handle_query_read = [this, self](boost::system::error_code ec, size_t length) {
//      if (!ec) {
//        send_row_description(_pg_handler.handle_query_packet(_input_packet, length));
//      }
//    };

    boost::system::error_code ec;
    auto bytes_read = _socket.receive(boost::asio::buffer(_input_packet.data), 0, ec);

    if (bytes_read == 0) {
      std::cout << "No bytes read!" << std::endl;
    }
    send_row_description();
  }

  void send_row_description() {
    auto self(shared_from_this());

    auto ready_for_next_query = [this, self](boost::system::error_code ec, size_t length) {
      if (!ec) {
        read_startup_packet();
      }
    };

    _output_packet.data.clear();
//    _pg_handler.write_string(_output_packet, sql + '\n');
    auto query = boost::asio::buffer(_output_packet.data);
    boost::asio::write(_socket, query);
    read_query();
  }

  tcp::socket _socket;
  static const uint16_t _max_length = 1024;
  PostgresWireHandler _pg_handler;
  InputPacket _input_packet;
  OutputPacket _output_packet;
};

class server {
 public:
  server(boost::asio::io_service& io_service, unsigned short port)
    : acceptor_(io_service, tcp::endpoint(tcp::v4(), port)),
      socket_(io_service) {
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

int main(int argc, char*argv[]) {
  try {
    if (argc != 2) {
      std::cerr << "Usage: async_tcp_echo_server <port>\n";
      return 1;
    }

    boost::asio::io_service io_service;

    opossum::server s(io_service, std::atoi(argv[1]));

    io_service.run();
  }
  catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
