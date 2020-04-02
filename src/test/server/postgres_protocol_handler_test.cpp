#include <algorithm>

#include "base_test.hpp"
#include "mock_socket.hpp"

#include "server/postgres_protocol_handler.hpp"

namespace opossum {

class PostgresProtocolHandlerTest : public BaseTest {
 protected:
  void SetUp() override {
    _mocked_socket = std::make_shared<MockSocket>();
    _protocol_handler =
        std::make_shared<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>>(_mocked_socket->get_socket());
  }

  std::shared_ptr<MockSocket> _mocked_socket;
  std::shared_ptr<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>> _protocol_handler;
};

TEST_F(PostgresProtocolHandlerTest, ReadStartupMessage) {
  // No SSL request, just length (8 Byte) and no SSL (0)
  // Values must be converted to network byte order
  _mocked_socket->write(std::string{'\0', '\0', '\0', '\b', '\0', '\0', '\0', '\0'});
  EXPECT_EQ(_protocol_handler->read_startup_packet_header(), 0);

  // SSL request contains length (8 B) and SSL request code 80877103
  _mocked_socket->write(std::string{'\0', '\0', '\0', '\b', '\x04', '\xd2', '\x16', '\x2f'});
  // Server will wait for new message with authentication details. Message contains length (12 B), protocol (0) and
  // body (4 B). No body provided here, since we throw it away anyway.
  _mocked_socket->write(std::string{'\0', '\0', '\0', '\f', '\0', '\0', '\0', '\0'});
  EXPECT_EQ(_protocol_handler->read_startup_packet_header(), 4);
  const std::string file_content = _mocked_socket->read();
  EXPECT_EQ(file_content.back(), 'N');
}

TEST_F(PostgresProtocolHandlerTest, DiscardStartupPacketBody) {
  // Write string including type of new packet, discard them, and see if packet type get correctly detected
  const std::string content = "garbageQ";
  _mocked_socket->write(content);
  _protocol_handler->read_startup_packet_body(static_cast<uint32_t>(content.size() - 1));
  EXPECT_EQ(_protocol_handler->read_packet_type(), PostgresMessageType::SimpleQueryCommand);
}

TEST_F(PostgresProtocolHandlerTest, SendAuthenticationResponse) {
  _protocol_handler->send_authentication_response();
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(static_cast<PostgresMessageType>(file_content.front()), PostgresMessageType::AuthenticationRequest);
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, SendParameter) {
  _protocol_handler->send_parameter("key", "value");
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(static_cast<PostgresMessageType>(file_content.front()), PostgresMessageType::ParameterStatus);
  // Ignore length of packet and directly read values
  EXPECT_EQ(std::string(file_content, sizeof(PostgresMessageType) + sizeof(uint32_t), 3u), "key");
  // Ignore null terminator
  EXPECT_EQ(std::string(file_content, sizeof(PostgresMessageType) + sizeof(uint32_t) + 3u + sizeof('\0'), 5u), "value");
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, SendReadyForQuery) {
  _protocol_handler->send_ready_for_query();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(static_cast<PostgresMessageType>(file_content.front()), PostgresMessageType::ReadyForQuery);
  EXPECT_EQ(static_cast<TransactionStatusIndicator>(file_content.back()), TransactionStatusIndicator::Idle);
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, GetMessageType) {
  _mocked_socket->write("Q");
  EXPECT_EQ(_protocol_handler->read_packet_type(), PostgresMessageType::SimpleQueryCommand);
}

TEST_F(PostgresProtocolHandlerTest, ReadQueryPacket) {
  // Write string including type of new packet, discard them, and see if packet type get correctly detected
  const std::string query = "SELECT 1;";
  _mocked_socket->write(std::string{'\0', '\0', '\0', '\x0e'});
  _mocked_socket->write(query);
  _mocked_socket->write(std::string{"\0", 1});
  EXPECT_EQ(_protocol_handler->read_query_packet(), query);
}

TEST_F(PostgresProtocolHandlerTest, SendRowDescriptionHeader) {
  const auto total_column_name_length = 42;
  const auto column_count = 5;
  _protocol_handler->send_row_description_header(total_column_name_length, column_count);
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(static_cast<PostgresMessageType>(file_content.front()), PostgresMessageType::RowDescription);
  EXPECT_EQ(
      NetworkConversionHelper::get_small_int(file_content.begin() + sizeof(uint32_t) + sizeof(PostgresMessageType)),
      column_count);
}

TEST_F(PostgresProtocolHandlerTest, SendRowDescription) {
  const std::string column_name = "test_column";
  const auto object_id = 23; /* data type Int*/

  _protocol_handler->send_row_description(column_name, object_id, sizeof(uint32_t));
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  auto start = 0;
  EXPECT_EQ(std::string(file_content.c_str(), column_name.size()), column_name);
  start += column_name.size() + sizeof('\0');
  // Misuse NetworkConversionHelper::get_message_length to get converted uint32_t
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.begin() + start), 0);
  start += sizeof(uint32_t);
  EXPECT_EQ(NetworkConversionHelper::get_small_int(file_content.begin() + start), 0);
  start += sizeof(uint16_t);
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.begin() + start), object_id);
  start += sizeof(uint32_t);
  EXPECT_EQ(NetworkConversionHelper::get_small_int(file_content.begin() + start), sizeof(uint32_t));
  start += sizeof(uint16_t);
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.begin() + start), -1);
  start += sizeof(uint32_t);
  EXPECT_EQ(NetworkConversionHelper::get_small_int(file_content.begin() + start), 0);
}

TEST_F(PostgresProtocolHandlerTest, SendValuesAsStrings) {
  const std::string value1 = "some";
  const std::string value2 = "string";

  _protocol_handler->send_data_row({value1, value2, std::nullopt},
                                   static_cast<uint32_t>(value1.size() + value2.size()));
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(static_cast<PostgresMessageType>(file_content.front()), PostgresMessageType::DataRow);
  auto start = sizeof(PostgresMessageType);
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.begin() + start), file_content.size() - 1);
  start += sizeof(uint32_t);
  // Amount of values
  EXPECT_EQ(NetworkConversionHelper::get_small_int(file_content.begin() + start), 3);
  start += sizeof(uint16_t);
  // Length of first value
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.begin() + start), value1.size());
  start += sizeof(uint32_t);
  // First value
  EXPECT_EQ(std::string(file_content, start, value1.size()), value1);
  start += value1.size();
  // Length of second value
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.begin() + start), value2.size());
  start += sizeof(uint32_t);
  // Second value
  EXPECT_EQ(std::string(file_content, start, value2.size()), value2);
  start += value2.size();
  // NULL value is represented by setting length to -1 and ommiting a value
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.begin() + start), -1);
}

TEST_F(PostgresProtocolHandlerTest, SendCommandComplete) {
  const std::string completion_message = "SELECT 1";
  _protocol_handler->send_command_complete(completion_message);
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(static_cast<PostgresMessageType>(file_content.front()), PostgresMessageType::CommandComplete);
  EXPECT_EQ(std::string(file_content, sizeof(PostgresMessageType) + sizeof(uint32_t), completion_message.size()),
            completion_message);
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, ReadParsePacket) {
  const std::string statement_name = "test_statement";
  const std::string query = "SELECT 1;";
  _mocked_socket->write(std::string{'\0', '\0', '\0', '\x23'});
  _mocked_socket->write(statement_name);
  _mocked_socket->write(std::string{"\0", 1});
  _mocked_socket->write(query);
  _mocked_socket->write(std::string{"\0", 1});
  // Specify data type of parameter. This value has currently no effect, but needs to be read.
  _mocked_socket->write(std::string{'\0', '\x01'});
  _mocked_socket->write(std::string{'\0', '\0', '\0', '\x17'});

  const auto& [statement_name_read, query_read] = _protocol_handler->read_parse_packet();
  EXPECT_EQ(statement_name, statement_name_read);
  EXPECT_EQ(query, query_read);
}

TEST_F(PostgresProtocolHandlerTest, ReadSyncPacket) {
  _mocked_socket->write("0000");

  // Not much to test here
  EXPECT_NO_THROW(_protocol_handler->read_sync_packet());
}

TEST_F(PostgresProtocolHandlerTest, SendStatusMessage) {
  _protocol_handler->send_status_message(PostgresMessageType::BindComplete);
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(static_cast<PostgresMessageType>(file_content.front()), PostgresMessageType::BindComplete);
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, ReadDescribePacket) {
  // Write string including type of new packet, discard them, and see if packet type get correctly detected
  const std::string portal_name = "some_portal";
  _mocked_socket->write(std::string{'\0', '\0', '\0', '\x10'});
  // Portal descriptions are supported
  _mocked_socket->write("P");
  _mocked_socket->write(portal_name);

  EXPECT_NO_THROW(_protocol_handler->read_describe_packet());

  _mocked_socket->write(std::string{'\0', '\0', '\0', '\x10'});
  // Statement descriptions are not supported
  _mocked_socket->write("S");
  _mocked_socket->write(portal_name);

  EXPECT_THROW(_protocol_handler->read_describe_packet(), std::logic_error);
}

TEST_F(PostgresProtocolHandlerTest, ReadBindPacket) {
  const std::string portal = "test_portal";
  const std::string statement_name = "test_statement";

  _mocked_socket->write(std::string{'\0', '\0', '\0', '\x31'});
  _mocked_socket->write(portal);
  _mocked_socket->write(std::string{"\0", 1});
  _mocked_socket->write(statement_name);
  _mocked_socket->write(std::string{"\0", 1});
  // Number of parameter format codes, let's take one in this test case
  _mocked_socket->write(std::string{'\0', '\x01'});
  // Format code 0: text format
  _mocked_socket->write(std::string{"\0", 2});
  // Number of parameters, one again
  _mocked_socket->write(std::string{'\0', '\x01'});
  // Assuming length of 4 Byte
  _mocked_socket->write(std::string{'\0', '\0', '\0', '\x04'});
  // Set parameter to value "test"
  _mocked_socket->write("test");
  // Assuming one result column
  _mocked_socket->write(std::string{'\0', '\x01'});
  // Also format code 0: text format
  _mocked_socket->write(std::string{"\0", 2});

  const auto& statement_information = _protocol_handler->read_bind_packet();
  EXPECT_EQ(statement_information.portal, portal);
  EXPECT_EQ(statement_information.statement_name, statement_name);
  EXPECT_EQ(statement_information.parameters, std::vector<AllTypeVariant>{"test"});
}

TEST_F(PostgresProtocolHandlerTest, ReadExecutePacket) {
  // Write string including type of new packet, discard them, and see if packet type get correctly detected
  const std::string portal_name = "some_portal";
  _mocked_socket->write(std::string{'\0', '\0', '\0', '\x14'});
  _mocked_socket->write(portal_name);
  _mocked_socket->write({'\0', '\0', '\0', '\0', '\0'});

  EXPECT_EQ(_protocol_handler->read_execute_packet(), portal_name);
}

TEST_F(PostgresProtocolHandlerTest, SendErrorMessage) {
  const std::string error_description = "error";
  const auto error_message = ErrorMessage{{PostgresMessageType::HumanReadableError, error_description}};
  _protocol_handler->send_error_message(error_message);
  const std::string file_content = _mocked_socket->read();

  auto start = 0;
  EXPECT_EQ(static_cast<PostgresMessageType>(file_content[start]), PostgresMessageType::ErrorResponse);
  start += sizeof(uint32_t) + sizeof(PostgresMessageType);
  EXPECT_EQ(static_cast<PostgresMessageType>(file_content[start]), PostgresMessageType::HumanReadableError);
  start += sizeof(PostgresMessageType);
  EXPECT_EQ(std::string(file_content, start, error_description.size()), error_description);
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

TEST_F(PostgresProtocolHandlerTest, SendExecutionInfo) {
  const std::string execution_information = "Executed within 1s";
  _protocol_handler->send_execution_info(execution_information);
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  auto start = 0;
  EXPECT_EQ(static_cast<PostgresMessageType>(file_content[start]), PostgresMessageType::Notice);
  start += sizeof(uint32_t) + sizeof(PostgresMessageType);
  EXPECT_EQ(static_cast<PostgresMessageType>(file_content[start]), PostgresMessageType::HumanReadableError);
  start += sizeof(PostgresMessageType);
  EXPECT_EQ(std::string(file_content, start, execution_information.size()), execution_information);
  EXPECT_EQ(NetworkConversionHelper::get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
}

}  // namespace opossum
