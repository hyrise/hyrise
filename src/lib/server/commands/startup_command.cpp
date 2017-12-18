#include "startup_command.hpp"

#include <boost/asio/buffer.hpp>

#include <server/hyrise_session.hpp>
#include <utils/assert.hpp>

namespace opossum {

void StartupCommand::start(std::size_t size) {
  // If we have no further content, this is a special 8 byte SSL packet,
  // which we decline with 'N' because we don't support SSL
  if (size == 0) {
    OutputPacket output_packet;
    PostgresWireHandler::write_value(output_packet, NetworkMessageType::SslNo);
    _session.async_send_packet(output_packet);

    _state = StartupCommandState::SslDeniedSent;
  } else {
    _state = StartupCommandState::ExpectingStartupContent;
    _session.async_receive_packet(size);
  }
}

void StartupCommand::handle_packet_received(const InputPacket& input_packet, std::size_t size) {
  switch (_state) {
    case StartupCommandState::ExpectingStartupContent:
      // We've received the startup package contents
      PostgresWireHandler::handle_startup_package_content(input_packet, size);
      send_auth();
      break;
    default:
      DebugAssert(false, "Received a packet at an unknown stage in the command");
      break;
  }
}

void StartupCommand::send_auth() {
  // This packet is our AuthenticationOK, which means we do not require any auth.
  OutputPacket output_packet;
  PostgresWireHandler::write_value(output_packet, NetworkMessageType::AuthenticationRequest);
  PostgresWireHandler::write_value(output_packet, htonl(8u));
  PostgresWireHandler::write_value(output_packet, htonl(0u));

  _session.async_send_packet(output_packet);
  _state = StartupCommandState::AuthenticationStateSent;

  // This is a Status Parameter which is useless but it seems like clients expect at least one. Contains dummy value.
  //    _output_packet.data.clear();
  //    _pg_handler.write_value(_output_packet, 'S');
  //    _pg_handler.write_value<uint32_t>(_output_packet, htonl(28u));
  //    _pg_handler.write_string(_output_packet, "client_encoding");
  //    _pg_handler.write_string(_output_packet, "UNICODE");
  //    auto params = boost::asio::buffer(_output_packet.data);
  //    boost::asio::write(_socket, params);
}

void StartupCommand::handle_packet_sent() {
  switch (_state) {
    case StartupCommandState::SslDeniedSent:
      // Wait for actual startup packet
      _session.terminate_command();
      return;
    case StartupCommandState::AuthenticationStateSent:
      _session.set_started();
      _session.terminate_command();
      break;
    default:
      DebugAssert(false, "Sent a packet at an unknown stage in the command");
      break;
  }
}

}  // namespace opossum
