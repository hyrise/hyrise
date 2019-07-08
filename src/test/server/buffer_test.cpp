// #include <memory>

#include <gmock/gmock.h>
#include "base_test.hpp"
#include "gtest/gtest.h"
#include <arpa/inet.h>

// #include "mock_connection.hpp"

#include "boost_server/buffer.hpp"

namespace opossum {

using ::testing::NiceMock;

class BufferTest : public BaseTest {
 public:
  void SetUp() override {
    boost::asio::io_service service;
    auto socket = std::make_shared<Socket>(service, boost::asio::ip::tcp::v4());
    service.run();
    _write_buffer = std::make_shared<WriteBuffer>(std::move(socket));
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

    _write_buffer = std::make_shared<WriteBuffer>(socket);

  }
  std::shared_ptr<ReadBuffer> _read_buffer;
  std::shared_ptr<WriteBuffer> _write_buffer;
};

TEST_F(BufferTest, Write) {
  EXPECT_EQ(_write_buffer->size(), 0);
  const std::string numbers = "0123456789";
  _write_buffer->put_string(numbers);
  _write_buffer->put_string(numbers, false);
  EXPECT_EQ(_write_buffer->size(), 21);
  // for (auto i = 0; i < 25; i++) {
  //   std::cout << *(_write_buffer->data() + i) << std::endl;
  // }
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

TEST_F(BufferTest, CorrectNetworkConversion) {
    const uint32_t some_integer = 42;
    _write_buffer->put_value<uint32_t>(some_integer);
    uint32_t value_from_buffer;
    std::copy_n(_write_buffer->data(), sizeof(some_integer), reinterpret_cast<char*>(&value_from_buffer));

    EXPECT_EQ(some_integer, value_from_buffer);
    std::copy_n((_write_buffer->data()) + 4, sizeof(some_integer), reinterpret_cast<char*>(&value_from_buffer));

    EXPECT_EQ(some_integer, value_from_buffer);
}

}  // namespace opossum
