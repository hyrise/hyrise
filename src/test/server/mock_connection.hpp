#pragma once

#include <gmock/gmock.h>
// #include <boost/thread/future.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include "server/buffer.hpp"
#include "server/postgres_protocol_handler.hpp"

namespace opossum {
using ::testing::_;
using ::testing::Invoke;

class MockIO : public boost::asio::io_service {
 public:
  // MockSocket() : boost::asio::ip::tcp::socket(boost::asio::io_context()) {}
  // MOCK_METHOD0(read, void());
  // // MockSocket() {}
};

class MockSocket : public boost::asio::ip::tcp::socket {
 public:
  MockSocket(boost::asio::io_service& service) : boost::asio::ip::tcp::socket(service) {}
  //     MOCK_METHOD1(read_some, size_t(size_t b));
  //     // MOCK_METHOD1(write_some, size_t(MutableBufferSequence b));
  //     // basic_streambuf< Allocator > & b,
  //     // CompletionCondition completion_condition,
  //     // boost::system::error_code & ec));
  //     // // MockSocket() {}

  //     // using bla = boost::asio::read;
  //     // MOCK_METHOD0(bla, size_t(
  //     //     boost::asio::SyncReadStream & s,
  //     //     boost::asio::basic_streambuf & b,
  //     // CompletionCondition completion_condition))
};
// class MockPG : public PostgresHandler {
// public:
//     MockPG() : PostgresHandler(MockSocket(MockIO)) {}
//     // MOCK_METHOD0(PostgresHandler, void(MockSocket))
// };

// class MockWriteBuffer : public WriteBuffer {
// // class MockWriteBuffer : public WriteBuffer {
// public:
//     MockWriteBuffer(std::shared_ptr<boost::asio::ip::tcp::socket> socket) : WriteBuffer(socket) {}
//     MOCK_METHOD1(flush, void(const size_t));

// };

class MockFoo {
 public:
  MockFoo(std::shared_ptr<boost::asio::ip::tcp::socket> socket) : _real(socket) {
    // By default, all calls are delegated to the real object
    ON_CALL(*this, put_string(_, _)).WillByDefault(Invoke(&_real, &WriteBuffer::put_string));
  }
  MOCK_METHOD2(put_string, void(const std::string&, const bool));
  MOCK_METHOD1(_flush_if_necessary, void(const size_t));

 private:
  WriteBuffer _real;
};

// class MockConnection {
//  public:
//   MOCK_METHOD0(receive_startup_packet_header, boost::future<uint32_t>());
//   MOCK_METHOD1(receive_startup_packet_body, boost::future<void>(uint32_t size));

//   MOCK_METHOD0(receive_packet_header, boost::future<RequestHeader>());
//   MOCK_METHOD1(receive_simple_query_packet_body, boost::future<std::string>(uint32_t size));
//   MOCK_METHOD1(receive_parse_packet_body, boost::future<ParsePacket>(uint32_t size));
//   MOCK_METHOD1(receive_bind_packet_body, boost::future<BindPacket>(uint32_t size));
//   MOCK_METHOD1(receive_describe_packet_body, boost::future<std::string>(uint32_t size));
//   MOCK_METHOD1(receive_sync_packet_body, boost::future<void>(uint32_t size));
//   MOCK_METHOD1(receive_flush_packet_body, boost::future<void>(uint32_t size));
//   MOCK_METHOD1(receive_execute_packet_body, boost::future<std::string>(uint32_t size));

//   MOCK_METHOD0(send_ssl_denied, boost::future<void>());
//   MOCK_METHOD0(send_auth, boost::future<void>());
//   MOCK_METHOD2(send_parameter_status, boost::future<void>(const std::string& key, const std::string& value));
//   MOCK_METHOD0(send_ready_for_query, boost::future<void>());
//   MOCK_METHOD1(send_error, boost::future<void>(const std::string& message));
//   MOCK_METHOD1(send_notice, boost::future<void>(const std::string& notice));
//   MOCK_METHOD1(send_status_message, boost::future<void>(const NetworkMessageType& type));
//   MOCK_METHOD1(send_row_description, boost::future<void>(const std::vector<ColumnDescription>& row_description));
//   MOCK_METHOD1(send_data_row, boost::future<void>(const std::vector<std::string>& row_strings));
//   MOCK_METHOD1(send_command_complete, boost::future<void>(const std::string& message));
// };

}  // namespace opossum
