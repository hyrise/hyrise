#include <boost/asio.hpp>
#include <algorithm>
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "mock_socket.hpp"

#include "server/postgres_protocol_handler.hpp"

namespace opossum {

class PostgresProtocolHandlerTest : public BaseTest {
 protected:
  void SetUp() override {
    _mocked_socket = std::make_shared<MockSocket>();
    _protocol_handler = std::make_shared<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>>(_mocked_socket->get_socket());
  }

  uint32_t _get_message_length(std::string::const_iterator start) {
    uint32_t network_value = 0;
    std::copy_n(start, sizeof(uint32_t), reinterpret_cast<char*>(&network_value));
    return ntohl(network_value);
  }

  std::shared_ptr<MockSocket> _mocked_socket;
  std::shared_ptr<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>> _protocol_handler;
};

TEST_F(PostgresProtocolHandlerTest, ReadStartupMessage) {
    // No SSL request, just length (8 Byte) and no SSL (0)
    // Values must be converted to network byte order
    _mocked_socket->write(std::string{'\0','\0','\0','\b','\0','\0','\0','\0'});
    EXPECT_EQ(_protocol_handler->read_startup_packet(), 0);

    // SSL request contains length (8 B) and SSL request code 80877103
    _mocked_socket->write(std::string{'\0','\0','\0','\b','\x04','\xd2','\x16','\x2f'});
    // Server will wait for new message with authentication details. Message contains length (12 B), protocol (0) and
    // body (4 B). No body provided here, since we throw it away anyway.
    _mocked_socket->write(std::string{'\0','\0','\0','\f','\0','\0','\0','\0'});
    EXPECT_EQ(_protocol_handler->read_startup_packet(), 4);
    const std::string file_content = _mocked_socket->read();
    EXPECT_EQ(file_content.back(), 'N');
}

TEST_F(PostgresProtocolHandlerTest, GetMessageType) {
    _mocked_socket->write("Q");
    EXPECT_EQ(_protocol_handler->read_packet_type(), NetworkMessageType::SimpleQueryCommand);
}

TEST_F(PostgresProtocolHandlerTest, SendAuthentication) {
    _protocol_handler->send_authentication();
    _protocol_handler->force_flush();
    const std::string file_content = _mocked_socket->read();

   EXPECT_EQ(static_cast<NetworkMessageType>(file_content.front()), NetworkMessageType::AuthenticationRequest);
   EXPECT_EQ(_get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, DiscardStartupPacketBody) {
  // Write string including type of new packet, discard them, and see if packet type get correctly detected
  const std::string content = "garbageQ";
  _mocked_socket->write(content);
  _protocol_handler->read_startup_packet_body(static_cast<uint32_t>(content.size() - 1));
  EXPECT_EQ(_protocol_handler->read_packet_type(), NetworkMessageType::SimpleQueryCommand);
}

TEST_F(PostgresProtocolHandlerTest, ReadQueryPacket) {
  // Write string including type of new packet, discard them, and see if packet type get correctly detected
  const std::string query = "SELECT 1;";
  _mocked_socket->write(std::string{'\0','\0','\0','\x0e'});
  _mocked_socket->write(query);
  _mocked_socket->write(std::string{"\0", 1});
  EXPECT_EQ(_protocol_handler->read_query_packet(), query);
}

TEST_F(PostgresProtocolHandlerTest, SendParameter) {
  _protocol_handler->send_parameter("key", "value");
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(static_cast<NetworkMessageType>(file_content.front()), NetworkMessageType::ParameterStatus);
  // Ignore length of packet and directly read values
  EXPECT_EQ(std::string(file_content, sizeof(NetworkMessageType) + sizeof(uint32_t), 3u), "key");
  // Ignore null terminator
  EXPECT_EQ(std::string(file_content, sizeof(NetworkMessageType) + sizeof(uint32_t) + 3u + sizeof('\0'), 5u), "value");
  EXPECT_EQ(_get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, SendReadyForQuery) {
  _protocol_handler->send_ready_for_query();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(static_cast<NetworkMessageType>(file_content.front()), NetworkMessageType::ReadyForQuery);
  EXPECT_EQ(static_cast<TransactionStatusIndicator>(file_content.back()), TransactionStatusIndicator::Idle);
  EXPECT_EQ(_get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, SendCommandComplete) {
  const std::string completion_message = "SELECT 1";
  _protocol_handler->send_command_complete(completion_message);
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();
  
  EXPECT_EQ(static_cast<NetworkMessageType>(file_content.front()), NetworkMessageType::CommandComplete);
  EXPECT_EQ(std::string(file_content, sizeof(NetworkMessageType) + sizeof(uint32_t), completion_message.size()), completion_message);
  EXPECT_EQ(_get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, SendStatusMessage) {
  _protocol_handler->send_status_message(NetworkMessageType::BindComplete);
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();
  EXPECT_EQ(static_cast<NetworkMessageType>(file_content.front()), NetworkMessageType::BindComplete);
  EXPECT_EQ(_get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, SendExecutionInfo) {
  const std::string execution_information = "Executed within 1s";
  _protocol_handler->send_execution_info(execution_information);
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();
  
  EXPECT_EQ(static_cast<NetworkMessageType>(file_content.front()), NetworkMessageType::Notice);
  EXPECT_EQ(static_cast<NetworkMessageType>(file_content[sizeof(uint32_t) + sizeof(NetworkMessageType)]), NetworkMessageType::HumanReadableError);
  EXPECT_EQ(std::string(file_content, sizeof(NetworkMessageType) + sizeof(uint32_t) + sizeof(NetworkMessageType), execution_information.size()), execution_information);
  EXPECT_EQ(_get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}


} // namespace opossum
