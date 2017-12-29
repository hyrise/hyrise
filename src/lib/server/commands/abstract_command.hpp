#pragma once

#include <server/postgres_wire_handler.hpp>

namespace opossum {

class HyriseSession;

class AbstractCommand {
 public:
  explicit AbstractCommand(HyriseSession& session) : _session(session) {}
  virtual ~AbstractCommand() = default;

  static const uint32_t HEADER_LENGTH = 5u;

  virtual void start(std::size_t size) = 0;
  virtual void handle_packet_received(const InputPacket& input_packet, std::size_t size);
  virtual void handle_packet_sent();
  virtual void handle_event_received();

 protected:
  HyriseSession& _session;
};

}  // namespace opossum
