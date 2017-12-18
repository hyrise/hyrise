#pragma once

#include <memory>
#include <optional>

#include <boost/asio/ip/tcp.hpp>

#include "postgres_wire_handler.hpp"
#include "types.hpp"

namespace opossum {

using boost::asio::ip::tcp;

class AbstractCommand;

class HyriseSession : public std::enable_shared_from_this<HyriseSession> {
 public:
  explicit HyriseSession(tcp::socket socket)
      : _socket(std::move(socket)),
        _input_packet(),
        _is_started(false),
        _expected_input_packet_length(0),
        _current_command(),
        _self() {}

  void start();

  // Interface used by Commands
  void set_started() { _is_started = true; }

  void async_receive_packet(std::size_t size);
  void async_send_packet(const OutputPacket& output_packet);

  void terminate_command();

 private:
  void handle_packet_received(const boost::system::error_code& error, size_t bytes_transferred);
  void handle_packet_sent(const boost::system::error_code& error);

  void async_send_ready_for_query();

  void terminate_session();

  tcp::socket _socket;
  InputPacket _input_packet;

  bool _is_started;
  std::size_t _expected_input_packet_length{};
  std::shared_ptr<AbstractCommand> _current_command;
  std::shared_ptr<HyriseSession> _self;
};

}  // namespace opossum
