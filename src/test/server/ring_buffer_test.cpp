#include <arpa/inet.h>
#include <boost/asio.hpp>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "mock_socket.hpp"

#include "server/ring_buffer.hpp"

namespace opossum {

class RingBufferTest : public BaseTest {
 protected:
  void SetUp() override {
    _mocked_socket = std::make_shared<MockSocket>();
    _read_buffer = std::make_shared<ReadBuffer<boost::asio::posix::stream_descriptor>>(_mocked_socket->get_socket());
    _write_buffer = std::make_shared<WriteBuffer<boost::asio::posix::stream_descriptor>>(_mocked_socket->get_socket());
  }

  RingBuffer _ring_buffer;
  std::shared_ptr<MockSocket> _mocked_socket;
  std::shared_ptr<ReadBuffer<boost::asio::posix::stream_descriptor>> _read_buffer;
  std::shared_ptr<WriteBuffer<boost::asio::posix::stream_descriptor>> _write_buffer;
};

TEST_F(RingBufferTest, GetSize) { EXPECT_EQ(_ring_buffer.size(), 0u); }

TEST_F(RingBufferTest, Full) { EXPECT_FALSE(_ring_buffer.full()); }

TEST_F(RingBufferTest, MaximumSize) { EXPECT_EQ(_ring_buffer.maximum_capacity(), BUFFER_SIZE - 1); }

TEST_F(RingBufferTest, ReadValues) {
  const auto converted_short = htons(16);
  const auto converted = htonl(32);
  const uint64_t long_value = 64;
  _mocked_socket->write(std::string(reinterpret_cast<const char*>(&converted_short), sizeof(uint16_t)));
  _mocked_socket->write(std::string(reinterpret_cast<const char*>(&converted), sizeof(uint32_t)));
  _mocked_socket->write(std::string(reinterpret_cast<const char*>(&long_value), sizeof(uint64_t)));
  _mocked_socket->write("A");

  EXPECT_EQ(_read_buffer->get_value<uint16_t>(), 16);
  EXPECT_EQ(_read_buffer->get_value<uint32_t>(), 32);
  EXPECT_EQ(_read_buffer->get_value<uint64_t>(), 64);
  EXPECT_EQ(_read_buffer->get_value<char>(), 'A');
}

TEST_F(RingBufferTest, ReadString) {
  const std::string original_content = {"somerandom\0string\0", 19};
  _mocked_socket->write(original_content);
  EXPECT_EQ(_read_buffer->get_string(4, false), "some");
  EXPECT_EQ(_read_buffer->get_string(), "random");
  EXPECT_EQ(_read_buffer->get_string(7), "string");
  const std::string buffer_content = {_read_buffer->data(), 19};
  EXPECT_EQ(buffer_content, original_content);
}

TEST_F(RingBufferTest, ReadLargeString) {
  const std::string original_content = std::string(BUFFER_SIZE + 2u, 'a');
  _mocked_socket->write(original_content);
  EXPECT_EQ(_read_buffer->get_string(original_content.size(), false), original_content);
  _mocked_socket->write(original_content);
  _mocked_socket->write(std::string{"\0", 1});
  EXPECT_EQ(_read_buffer->get_string(), original_content);
}

TEST_F(RingBufferTest, ReadPostgresMessageType) {
  _mocked_socket->write("Q");
  EXPECT_EQ(_read_buffer->get_message_type(), PostgresMessageType::SimpleQueryCommand);
}

TEST_F(RingBufferTest, WriteValues) {
  _write_buffer->put_value<uint16_t>(16);
  _write_buffer->put_value<uint32_t>(32);
  _write_buffer->put_value<uint64_t>(64);
  _write_buffer->put_value('A');
  // Manual flush because buffer isn't full
  _write_buffer->flush();
  const auto file_content = _mocked_socket->read();

  uint16_t small_value = 0;
  std::copy_n(file_content.begin(), sizeof(uint16_t), reinterpret_cast<char*>(&small_value));
  EXPECT_EQ(ntohs(small_value), 16);

  uint32_t value = 0;
  std::copy_n(file_content.begin() + sizeof(uint16_t), sizeof(uint32_t), reinterpret_cast<char*>(&value));
  EXPECT_EQ(ntohl(value), 32);

  uint64_t long_value = 0;
  std::copy_n(file_content.begin() + sizeof(uint16_t) + sizeof(uint32_t), +sizeof(uint64_t),
              reinterpret_cast<char*>(&long_value));
  EXPECT_EQ(long_value, 64);

  EXPECT_EQ(file_content.back(), 'A');
}

TEST_F(RingBufferTest, WriteString) {
  _write_buffer->put_string("somerandom");
  _write_buffer->put_string("string", false);
  const std::string whole_content = {"somerandom\0string", 17};
  const std::string buffer_content = {_write_buffer->data(), 17};
  EXPECT_EQ(buffer_content, whole_content);
  _write_buffer->flush();
  EXPECT_EQ(_mocked_socket->read(), whole_content);
}

TEST_F(RingBufferTest, WriteLargeString) {
  const std::string original_content = std::string(BUFFER_SIZE + 2u, 'a');
  _write_buffer->put_string(original_content, false);
  _write_buffer->flush();
  EXPECT_EQ(_mocked_socket->read(), original_content);
}

}  // namespace opossum
