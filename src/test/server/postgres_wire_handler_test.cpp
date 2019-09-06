#include <gmock/gmock.h>
#include <base_test.hpp>
#include "mock_connection.hpp"
#include "server/postgres_handler.hpp"

namespace opossum {

using ::testing::NiceMock;

class PostgresHandlerTest : public BaseTest {
 protected:
  void SetUp() override {
    // _socket = std::make_shared<NiceMock<MockSocket>>(NiceMock<MockSocket>);
    NiceMock<MockIO> io;
    // NiceMock<MockSocket> socket(io);
    auto socket = MockSocket(io);
    auto _socket = std::make_shared<MockSocket>(std::move(socket));

    // std::shared_ptr<Socket> socket(NiceMock<MockSocket>());
    _postgres_protocol_handler = std::make_shared<PostgresHandler>(PostgresHandler(_socket));
  }
  // std::shared_ptr<Socket> _socket;
  std::shared_ptr<PostgresHandler> _postgres_protocol_handler;
  // PostgresHandler _postgres_protocol_handler;
};

TEST_F(PostgresHandlerTest, HandleQueryPacketEmpty) {
  size_t length = 0;

  const std::string result = _postgres_protocol_handler->read_query_packet();

  ASSERT_EQ(result.size(), length);
}

// TEST_F(PostgresHandlerTest, HandleQueryPacket) {
//   ByteBuffer buffer = {'Q', 'u', 'e', 'r', 'y', '\0'};
//   _input_packet.data = buffer;
//   _input_packet.offset = _input_packet.data.cbegin();

//   std::string result = postgres_wire_handler.handle_query_packet(_input_packet);

//   ASSERT_GT(result.length(), 0ul);
//   ASSERT_TRUE(result.find("Query") != std::string::npos);
// }

// TEST_F(PostgresHandlerTest, HandleHeader) {
//   ByteBuffer buffer = {'Q'};
//   uint32_t value = ntohl(100);
//   char* chars = reinterpret_cast<char*>(&value);
//   buffer.insert(buffer.end(), chars, chars + sizeof(uint32_t));
//   _input_packet.data = buffer;
//   _input_packet.offset = _input_packet.data.cbegin();

//   auto command_header = PostgresWireHandler::handle_header(_input_packet);

//   ASSERT_EQ(command_header.message_type, NetworkMessageType::SimpleQueryCommand);
//   // length is 100 - 4 because the 4 bytes used to store the length itself are subtracted
//   ASSERT_EQ(command_header.payload_length, 96ul);
// }

// TEST_F(PostgresHandlerTest, HandleStartupPackage) {
//   ByteBuffer buffer = {};
//   uint32_t value = ntohl(100);
//   char* chars = reinterpret_cast<char*>(&value);
//   buffer.insert(buffer.end(), chars, chars + sizeof(uint32_t));  // length
//   buffer.insert(buffer.end(), chars, chars + sizeof(uint32_t));  // version
//   _input_packet.data = buffer;
//   _input_packet.offset = _input_packet.data.cbegin();

//   uint32_t result = postgres_wire_handler.handle_startup_package(_input_packet);
//   ASSERT_EQ(result, 92ul);  // 100 - 2 * sizeof(uint32_t)
// }

// TEST_F(PostgresHandlerTest, WriteString) {
//   std::string value("Response");

//   postgres_wire_handler.write_string(_output_packet, value, true);

//   ASSERT_EQ(_output_packet.data.size(), value.length() + 1);
//   // string should be terminated
//   ASSERT_EQ(_output_packet.data[value.length()], '\0');
// }
}  // namespace opossum
