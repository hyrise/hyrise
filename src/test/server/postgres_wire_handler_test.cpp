
#include <base_test.hpp>
#include <server/postgres_wire_handler.hpp>

namespace opossum {

class PostgresWireHandlerTest : public BaseTest {
 protected:
  PostgresWireHandler postgres_wire_handler;
  InputPacket _input_packet;
  OutputPacket _output_packet;
};

TEST_F(PostgresWireHandlerTest, HandleQueryPacketEmpty) {
  size_t length = 0;

  std::string result = postgres_wire_handler.handle_query_packet(_input_packet);

  ASSERT_EQ(result.length(), length);
}

TEST_F(PostgresWireHandlerTest, HandleQueryPacket) {
  ByteBuffer buffer = {'Q', 'u', 'e', 'r', 'y'};
  _input_packet.data = buffer;
  _input_packet.offset = _input_packet.data.cbegin();

  std::string result = postgres_wire_handler.handle_query_packet(_input_packet);

  ASSERT_GT(result.length(), 0ul);
  ASSERT_TRUE(result.find("Query") != std::string::npos);
}

TEST_F(PostgresWireHandlerTest, HandleHeader) {
  ByteBuffer buffer = {'Q'};
  uint32_t value = ntohl(100);
  char* chars = reinterpret_cast<char*>(&value);
  buffer.insert(buffer.end(), chars, chars + sizeof(uint32_t));
  _input_packet.data = buffer;
  _input_packet.offset = _input_packet.data.cbegin();

  auto command_header = PostgresWireHandler::handle_header(_input_packet);

  ASSERT_EQ(command_header.message_type, NetworkMessageType::SimpleQueryCommand);
  // length is 100 - 4 because the 4 bytes used to store the length itself are subtracted
  ASSERT_EQ(command_header.payload_length, 96ul);
}

TEST_F(PostgresWireHandlerTest, HandleStartupPackage) {
  ByteBuffer buffer = {};
  uint32_t value = ntohl(100);
  char* chars = reinterpret_cast<char*>(&value);
  buffer.insert(buffer.end(), chars, chars + sizeof(uint32_t));  // length
  buffer.insert(buffer.end(), chars, chars + sizeof(uint32_t));  // version
  _input_packet.data = buffer;
  _input_packet.offset = _input_packet.data.cbegin();

  uint32_t result = postgres_wire_handler.handle_startup_package(_input_packet);
  ASSERT_EQ(result, 92ul);  // 100 - 2 * sizeof(uint32_t)
}

TEST_F(PostgresWireHandlerTest, WriteString) {
  std::string value("Response");

  postgres_wire_handler.write_string(_output_packet, value, true);

  ASSERT_EQ(_output_packet.data.size(), value.length() + 1);
  // string should be terminated
  ASSERT_EQ(_output_packet.data[value.length()], '\0');
}
}  // namespace opossum
