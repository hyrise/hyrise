
#include <base_test.hpp>
#include <server/postgres_wire_handler.hpp>

namespace opossum {

class PostgresWireHandlerTest : public BaseTest {
 protected:
  PostgresWireHandler postgres_wire_handler;
  InputPacket _input_packet;
};

TEST_F(PostgresWireHandlerTest, HandleQueryPacketEmpty) {
  size_t length = 0;

  std::string result = postgres_wire_handler.handle_query_packet(_input_packet, length);

  ASSERT_EQ(result.length(), 0);
}

TEST_F(PostgresWireHandlerTest, HandleQueryPacket) {
  ByteBuffer buffer = {'Q', 'u', 'e', 'r', 'y'};
  size_t length = buffer.size();
  _input_packet.data = buffer;

  std::string result = postgres_wire_handler.handle_query_packet(_input_packet, length);

  ASSERT_GT(result.length(), 0);
  ASSERT_TRUE(result.find("Query") != std::string::npos);
}

TEST_F(PostgresWireHandlerTest, HandleHeader) {
  ByteBuffer buffer = {'Q'};
  uint32_t value = ntohl(100);
  char* chars = reinterpret_cast<char*>(&value);
  buffer.insert(buffer.end(), chars, chars + sizeof(uint32_t));
  _input_packet.data = buffer;

  auto command_header = PostgresWireHandler::handle_header(_input_packet);

  ASSERT_EQ(command_header.message_type, NetworkMessageType::SimpleQueryCommand);
  // length is 100 - 4 because the 4 bytes used to store the length itself are subtracted
  ASSERT_EQ(command_header.payload_length, 96);
}
}  // namespace opossum
