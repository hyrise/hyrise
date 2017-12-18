#pragma once

#include "abstract_command.hpp"

namespace opossum {

class SimpleQueryCommand : public AbstractCommand {
 public:
  explicit SimpleQueryCommand(HyriseSession &session)
    : AbstractCommand(session)
    , _state(SimpleQueryCommandState::Started) {}

  void start(std::size_t size) override;
  void handle_packet_received(const InputPacket &input_packet, std::size_t size) override;
  void handle_packet_sent() override;

 protected:
  enum class SimpleQueryCommandState {
    Started,
    RowDescriptionSent,
    RowDataSent,
    CommandCompleteSent
  };

  void send_row_description(const std::string& sql);
  void send_row_data();
  void send_command_complete();

  SimpleQueryCommandState _state;
};

}  // namespace opossum