// Files in the /bin folder are not tested. Everything that can be tested should be in the /lib folder and this file
// should be as short as possible.

#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>

#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class session
  : public std::enable_shared_from_this<session> {
 public:
  session(tcp::socket socket)
    : _socket(std::move(socket)) {
  }

  void start() {
    do_read();
  }

 private:
  void do_read() {
    auto self(shared_from_this());

    auto read_request_t = [this, self](boost::system::error_code ec, size_t length) {
      if (!ec) {
        read_request_type();
      }
    };
    _socket.async_read_some(boost::asio::buffer(_data, _max_length), read_request_t);
  }

  void read_request_type() {
    auto self(shared_from_this());

    auto write_response = [this, self](boost::system::error_code ec, size_t length) {
      if (!ec) {
        do_write(length);
      }
    };

    _socket.async_read_some(boost::asio::buffer(_data, _max_length), write_response);
  }

  void do_write(std::size_t length) {
    auto self(shared_from_this());

    auto ready_for_next_query = [this, self](boost::system::error_code ec, size_t length) {
      if (!ec) {
        do_read();
      }
    };

    boost::asio::async_write(_socket, boost::asio::buffer(_data, length), ready_for_next_query);
  }

  tcp::socket _socket;
  static const uint16_t _max_length = 1024;
  char _data[_max_length];
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
        std::make_shared<session>(std::move(socket_))->start();
      }
      do_accept();
    };

    acceptor_.async_accept(socket_, start_session);
  }

  tcp::acceptor acceptor_;
  tcp::socket socket_;
};

int main(int argc, char*argv[]) {
  try {
    if (argc != 2) {
      std::cerr << "Usage: async_tcp_echo_server <port>\n";
      return 1;
    }

    boost::asio::io_service io_service;

    server s(io_service, std::atoi(argv[1]));

    io_service.run();
  }
  catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}