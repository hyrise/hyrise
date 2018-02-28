#pragma once

#include <gmock/gmock.h>
#include <base_test.hpp>
#include <boost/thread/future.hpp>

namespace opossum {

class MockConnection {
 public:
  MOCK_METHOD0(receive_startup_packet_header, boost::future<uint32_t>());
  MOCK_METHOD1(receive_startup_packet_contents, boost::future<void>(uint32_t size));
  MOCK_METHOD0(receive_packet_header, boost::future<RequestHeader>());
  MOCK_METHOD1(receive_packet_contents, boost::future<InputPacket>(uint32_t size));

  MOCK_METHOD0(send_ssl_denied, boost::future<void>());
  MOCK_METHOD0(send_auth, boost::future<void>());
  MOCK_METHOD0(send_ready_for_query, boost::future<void>());
  MOCK_METHOD1(send_error, boost::future<void>(const std::string& message));
  MOCK_METHOD1(send_notice, boost::future<void>(const std::string& notice));
  MOCK_METHOD1(send_status_message, boost::future<void>(const NetworkMessageType& type));
  MOCK_METHOD1(send_row_description, boost::future<void>(const std::vector<ColumnDescription>& row_description));
  MOCK_METHOD1(send_data_row, boost::future<void>(const std::vector<std::string>& row_strings));
  MOCK_METHOD1(send_command_complete, boost::future<void>(const std::string& message));
};

}  // namespace opossum
