#pragma once

#include <boost/asio/ip/tcp.hpp>

#include "postgres_handler.hpp"

namespace opossum {

class Session {
 public:
  explicit Session(boost::asio::ip::tcp::socket socket);
  void start();

 private:
  void _establish_connection();

  void _handle_request();

  void _handle_simple_query();

  void _handle_parse_command();

  PostgresHandler _postgres_handler;
  bool _terminate_session = false;
};
}  // namespace opossum
