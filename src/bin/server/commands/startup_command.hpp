#pragma once

#include "abstract_command.hpp"

namespace opossum {

class StartupCommand : public AbstractCommand {
public:
  explicit StartupCommand(HyriseSession &session)
      : AbstractCommand(session)
      , _state(StartupCommandState::Started) {}

  void start(std::size_t size) override;
  void handle_packet_received(const InputPacket &input_packet, std::size_t size) override;
  void handle_packet_sent() override;

  static const uint32_t STARTUP_HEADER_LENGTH = 8u;

 protected:
  enum class StartupCommandState {
    Started,
    SslDeniedSent,
    ExpectingStartupContent,
    AuthenticationStateSent
  };

  void send_auth();

  StartupCommandState _state;
};

}  // namespace opossum