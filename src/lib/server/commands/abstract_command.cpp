#include "abstract_command.hpp"

#include <utils/assert.hpp>

namespace opossum {

void AbstractCommand::handle_packet_received(const InputPacket& input_packet, std::size_t size) {
  DebugAssert(false, "This command does not expect to receive packets");
}

void AbstractCommand::handle_packet_sent() { DebugAssert(false, "This command is not supposed to send packets"); }

void AbstractCommand::handle_event_received() { DebugAssert(false, "This command is not supposed to receive events"); }

}  // namespace opossum
