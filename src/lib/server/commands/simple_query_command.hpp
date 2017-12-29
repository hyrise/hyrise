#pragma once

#include "abstract_command.hpp"

namespace opossum {

class Table;

class SimpleQueryCommand : public AbstractCommand {
 public:
  explicit SimpleQueryCommand(HyriseSession& session)
      : AbstractCommand(session), _state(SimpleQueryCommandState::Started), _result_table(nullptr) {}

  void start(std::size_t size) override;
  void handle_packet_received(const InputPacket& input_packet, std::size_t size) override;
  void handle_packet_sent() override;
  void handle_event_received() override;

 protected:
  enum class SimpleQueryCommandState { Started, Executing, RowDescriptionSent, RowDataSent, CommandCompleteSent };

  void start_query(const std::string& sql);
  void send_row_description();
  void send_row_data();
  void send_command_complete();

  SimpleQueryCommandState _state;
  std::shared_ptr<const Table> _result_table;
};

}  // namespace opossum
