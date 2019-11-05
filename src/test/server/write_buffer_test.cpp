#include <boost/asio.hpp>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "mock_socket.hpp"

#include "server/write_buffer.hpp"

namespace opossum {

class WriteBufferTest : public BaseTest {
 protected:
  void SetUp() override {
    _mocked_socket = std::make_shared<MockSocket>();
    _write_buffer = std::make_shared<WriteBuffer<boost::asio::posix::stream_descriptor>>(_mocked_socket->get_socket());
  }

  std::shared_ptr<MockSocket> _mocked_socket;
  std::shared_ptr<WriteBuffer<boost::asio::posix::stream_descriptor>> _write_buffer;
};

TEST_F(WriteBufferTest, Full) {
  EXPECT_FALSE(_write_buffer->full());
  _write_buffer->put_string(std::string(_write_buffer->maximum_capacity(), 'a'), HasNullTerminator::No);
  EXPECT_TRUE(_write_buffer->full());
}

TEST_F(WriteBufferTest, GetSize) {
  EXPECT_EQ(_write_buffer->size(), 0u);
  _write_buffer->put_string("random", HasNullTerminator::No);
  EXPECT_EQ(_write_buffer->size(), 6u);
}

TEST_F(WriteBufferTest, WriteValues) {
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

TEST_F(WriteBufferTest, WriteString) {
  _write_buffer->put_string("somerandom");
  _write_buffer->put_string("string", HasNullTerminator::No);
  const std::string whole_content = {"somerandom\0string", 17};
  _write_buffer->flush();
  EXPECT_EQ(_mocked_socket->read(), whole_content);
}

TEST_F(WriteBufferTest, WriteLargeString) {
  const std::string original_content = std::string(SERVER_BUFFER_SIZE + 2u, 'a');
  _write_buffer->put_string(original_content, HasNullTerminator::No);
  // Since we use transfer_at_least when flushing the buffer we do not know how many bytes are going to written. It is
  // at least the amount bytes of the characters to be inserted and at maximum the buffer size.
  EXPECT_TRUE((original_content.size() - _write_buffer->maximum_capacity()) <= _mocked_socket->read().size() &&
              _mocked_socket->read().size() <= _write_buffer->maximum_capacity());
  _write_buffer->flush();
  EXPECT_EQ(_mocked_socket->read(), original_content);
}

}  // namespace opossum
