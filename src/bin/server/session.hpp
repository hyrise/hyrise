#pragma once

#include <iostream>
#include <memory>

#include <boost/asio.hpp>

#include "postgres_wire_handler.hpp"

namespace opossum {

using boost::asio::ip::tcp;

static const uint32_t HEADER_LENGTH = 5u;
static const uint32_t STARTUP_HEADER_LENGTH = 8u;

class Session : public std::enable_shared_from_this<Session> {
 public:
  explicit Session(tcp::socket socket) : _socket(std::move(socket)) {}

  void start();

 private:
  void read_startup_packet();
  void read_query();

  void send_auth();
  void send_ready_for_query();
  void send_row_description(const std::string& sql);
  void send_row_data();

  tcp::socket _socket;
  PostgresWireHandler _pg_handler;
  InputPacket _input_packet;
  OutputPacket _output_packet;
};

}  // namespace opossum
