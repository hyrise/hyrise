// #include <memory>

#include <arpa/inet.h>
#include <gmock/gmock.h>
#include "base_test.hpp"
#include "gtest/gtest.h"
#include <boost/asio.hpp>

#include "mock_connection.hpp"

#include "server/ring_buffer.hpp"

namespace opossum {

using ::testing::NiceMock;

class RingBufferTest : public BaseTest {
 public:
  void SetUp() override {
    // boost::asio::io_service service;

    // auto socket = std::make_shared<Socket>(service, boost::asio::ip::tcp::v4());
    // service.run();
    mocked_socket = std::make_shared<MockSocket>();
    _read_buffer = std::make_shared<ReadBuffer<boost::asio::posix::stream_descriptor>>(mocked_socket->get_socket());
    _write_buffer = std::make_shared<WriteBuffer<boost::asio::posix::stream_descriptor>>(mocked_socket->get_socket());
    // NiceMock<MockIO> io;
    // auto socket = MockSocket(io);
    // NiceMock<MockSocket> socket(io);

    // auto sockett = std::make_shared<Socket>(std::move(socket));
    // auto sockett = std::make_shared<NiceMock<MockSocket>>(NiceMock<MockSocket>(io));

    // sockett->open(boost::asio::ip::tcp::v4());
    // NiceMock<MockWriteBuffer> wbuff(sockett);
    // _write_buffer = std::make_shared<MockWriteBuffer>(wbuff);
    // _write_buffer = std::make_shared<MockWriteBuffer>();
    // const std::string numbers(BUFFER_SIZE + 5, '1');
    // NiceMock<MockFoo> buff(sockett);

    // buff.put_string(numbers,true);

    // _write_buffer = std::make_shared<WriteBuffer>(socket);
  }
protected:
    RingBuffer ring_buffer;
    std::shared_ptr<MockSocket> mocked_socket;
  std::shared_ptr<ReadBuffer<boost::asio::posix::stream_descriptor>> _read_buffer;
  std::shared_ptr<WriteBuffer<boost::asio::posix::stream_descriptor>> _write_buffer;
};


TEST_F(RingBufferTest, GetSize) {
  EXPECT_EQ(ring_buffer.size(), 0u);
}

TEST_F(RingBufferTest, Full) {
  EXPECT_FALSE(ring_buffer.full());
}

TEST_F(RingBufferTest, MaximumSize) {
  EXPECT_EQ(ring_buffer.maximum_capacity(), BUFFER_SIZE - 1);
}

// TEST_F(RingBufferTest, ReadValues) {
//     std::cout << 0 << std::endl;
//     // const std::string someval = std::to_string(htons(42));
//     std::cout << 1 << std::endl;
//   mocked_socket->write(std::string(reinterpret_cast<char*>(htons(42)), 2));
//     std::cout << 5 << std::endl;
  
//     std::cout << 2 << std::endl;
//   EXPECT_EQ(_read_buffer->get_value<uint16_t>(), 42);
//     std::cout << 3 << std::endl;
// }

TEST_F(RingBufferTest, ReadString) {
  const std::string original_content = std::string{"somerandom\0string\0", 19};
  mocked_socket->write(original_content);
  EXPECT_EQ(_read_buffer->get_string(4, false), "some");
  EXPECT_EQ(_read_buffer->get_string(), "random");
  EXPECT_EQ(_read_buffer->get_string(7), "string");
  const std::string buffer_content = {_read_buffer->data(), 19};
  EXPECT_EQ(buffer_content, original_content);
}

TEST_F(RingBufferTest, ReadNetworkMessageType) {
  mocked_socket->write("Q");
  EXPECT_EQ(_read_buffer->get_message_type(), NetworkMessageType::SimpleQueryCommand);
}





// TEST_F(BufferTest, WriteMoreThanAvailable) {
//   EXPECT_EQ(_write_buffer->size(), 0);
//   const std::string numbers(BUFFER_SIZE + 5, '1');
//   _write_buffer->put_string(numbers);
//   _write_buffer->put_string(numbers, false);
//   EXPECT_EQ(_write_buffer->size(), 21);
//   for (auto i = 0; i < 25; i++) {
//     std::cout << *(_write_buffer->data() + i) << std::endl;
//   }
// }

// TEST_F(RingBufferTest, CorrectNetworkConversion) {
//   const uint32_t some_integer = 42;
//   _write_buffer->put_value<uint32_t>(some_integer);
//   uint32_t value_from_buffer;
//   std::copy_n(_write_buffer->data(), sizeof(some_integer), reinterpret_cast<char*>(&value_from_buffer));
//   EXPECT_EQ(some_integer, ntohl(value_from_buffer));

//   const uint16_t some_small_integer = 42;
//   _write_buffer->put_value<uint16_t>(42);
//   uint16_t value_from_buffer1;
//   std::copy_n(_write_buffer->data() + sizeof(some_integer), sizeof(some_small_integer),
//               reinterpret_cast<char*>(&value_from_buffer1));
//   EXPECT_EQ(some_small_integer, ntohs(value_from_buffer1));

//   const uint64_t some_long_value = 42;
//   _write_buffer->put_value<uint64_t>(some_long_value);
//   uint64_t value_from_buffer2;
//   std::copy_n(_write_buffer->data() + sizeof(some_small_integer), sizeof(some_long_value),
//               reinterpret_cast<char*>(&value_from_buffer2));
//   EXPECT_EQ(some_long_value, value_from_buffer2);
// }

}  // namespace opossum
