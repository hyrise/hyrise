#include <boost/asio.hpp>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "mock_socket.hpp"

#include "server/read_buffer.hpp"

namespace opossum {

class ReadBufferTest : public BaseTest {
 protected:
  void SetUp() override {
    _mocked_socket = std::make_shared<MockSocket>();
    _read_buffer = std::make_shared<ReadBuffer<boost::asio::posix::stream_descriptor>>(_mocked_socket->get_socket());
  }

  std::shared_ptr<MockSocket> _mocked_socket;
  std::shared_ptr<ReadBuffer<boost::asio::posix::stream_descriptor>> _read_buffer;
};

TEST_F(ReadBufferTest, Full) {
  EXPECT_FALSE(_read_buffer->full());
  _mocked_socket->write(std::string(_read_buffer->maximum_capacity(), 'a'));
  _read_buffer->get_string(1, HasNullTerminator::No);
  EXPECT_FALSE(_read_buffer->full());
}

TEST_F(ReadBufferTest, GetSize) {
  EXPECT_EQ(_read_buffer->size(), 0u);
  _mocked_socket->write("random");
  _read_buffer->template get_value<char>();
  EXPECT_EQ(_read_buffer->size(), 5);
}

TEST_F(ReadBufferTest, ReadValues) {
  const auto converted_short = htons(16);
  const auto converted = htonl(32);
  _mocked_socket->write(std::string(reinterpret_cast<const char*>(&converted_short), sizeof(uint16_t)));
  _mocked_socket->write(std::string(reinterpret_cast<const char*>(&converted), sizeof(uint32_t)));
  _mocked_socket->write("A");

  EXPECT_EQ(_read_buffer->get_value<uint16_t>(), 16);
  EXPECT_EQ(_read_buffer->get_value<uint32_t>(), 32);
  EXPECT_EQ(_read_buffer->get_value<char>(), 'A');
}

TEST_F(ReadBufferTest, ReadString) {
  const std::string original_content = {"somerandom\0string\0", 19};
  _mocked_socket->write(original_content);
  EXPECT_EQ(_read_buffer->get_string(4, HasNullTerminator::No), "some");
  EXPECT_EQ(_read_buffer->get_string(), "random");
  EXPECT_EQ(_read_buffer->get_string(7), "string");
}

TEST_F(ReadBufferTest, ReadLargeString) {
  const std::string original_content = std::string(SERVER_BUFFER_SIZE + 2u, 'a');
  _mocked_socket->write(original_content);
  EXPECT_EQ(_read_buffer->get_string(original_content.size(), HasNullTerminator::No), original_content);
  _mocked_socket->write(original_content);
  _mocked_socket->write(std::string{"\0", 1});
  EXPECT_EQ(_read_buffer->get_string(), original_content);
}

}  // namespace opossum
