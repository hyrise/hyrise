#include "hyrise_session.hpp"

#include <iostream>

#include <boost/asio/placeholders.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>

#include <utils/assert.hpp>

#include "commands/abstract_command.hpp"
#include "commands/simple_query_command.hpp"
#include "commands/startup_command.hpp"
#include "types.hpp"

namespace opossum {

void HyriseSession::start() {
  // Keep a pointer to itself that will be released once the connection is closed
  _self = shared_from_this();

  async_receive_packet(StartupCommand::STARTUP_HEADER_LENGTH);
}

void HyriseSession::async_send_packet(const OutputPacket& output_packet) {
  boost::asio::async_write(_socket, boost::asio::buffer(output_packet.data),
                           boost::bind(&HyriseSession::handle_packet_sent, this, boost::asio::placeholders::error));
}

void HyriseSession::handle_packet_received(const boost::system::error_code& error, size_t bytes_transferred) {
  if (error) {
    std::cout << error.category().name() << ':' << error.value() << std::endl;
    DebugAssert(false, "An error occurred when reading from the connection");
  }

  DebugAssert(bytes_transferred == _expected_input_packet_length, "Client sent less data than expected.");

  if (_current_command) {
    _current_command->handle_packet_received(_input_packet, bytes_transferred);
    return;
  }

  if (!_is_started) {
    // This tells us how much more data there is in this packet
    auto startup_packet_length = PostgresWireHandler::handle_startup_package(_input_packet);
    _current_command = std::make_shared<StartupCommand>(*this);
    _current_command->start(startup_packet_length);
    return;
  }

  // We're currently idling, so read a new incoming message header
  auto command_header = PostgresWireHandler::handle_header(_input_packet);
  switch (command_header.message_type) {
    case NetworkMessageType::SimpleQueryCommand: {
      _current_command = std::make_shared<SimpleQueryCommand>(*this);
      break;
    }

    case NetworkMessageType::TerminateCommand: {
      // This immediately releases the session object
      terminate_session();
      return;
    }

    default: {
      std::cout << "Unknown command received: " << static_cast<unsigned char>(command_header.message_type) << std::endl;
    }
  }

  _current_command->start(command_header.payload_length);
}

void HyriseSession::terminate_command() {
  _current_command.reset();

  if (_is_started) {
    async_send_ready_for_query();
  } else {
    // Wait for the next startup request
    async_receive_packet(StartupCommand::STARTUP_HEADER_LENGTH);
  }
}

void HyriseSession::terminate_session() {
  _socket.close();
  _self.reset();
}

void HyriseSession::handle_packet_sent(const boost::system::error_code& error) {
  if (error) {
    std::cout << error.category().name() << ':' << error.value() << std::endl;
    DebugAssert(false, "An error occurred when writing to the connection");
  }

  if (_current_command) {
    _current_command->handle_packet_sent();
  } else {
    // This is the callback from sending the idle notification
    // Wait for the next request
    async_receive_packet(AbstractCommand::HEADER_LENGTH);
  }
}

void HyriseSession::async_receive_packet(std::size_t size) {
  _expected_input_packet_length = size;
  _input_packet.offset = _input_packet.data.begin();

  _socket.async_read_some(boost::asio::buffer(_input_packet.data, size),
                          boost::bind(&HyriseSession::handle_packet_received, this, boost::asio::placeholders::error,
                                      boost::asio::placeholders::bytes_transferred));
}

void HyriseSession::async_send_ready_for_query() {
  // ReadyForQuery packet 'Z' with transaction status Idle 'I'
  OutputPacket output_packet;
  PostgresWireHandler::write_value(output_packet, NetworkMessageType::ReadyForQuery);
  PostgresWireHandler::write_value(output_packet, htonl(5u));
  PostgresWireHandler::write_value(output_packet, TransactionStatusIndicator::Idle);

  async_send_packet(output_packet);
}

}  // namespace opossum
