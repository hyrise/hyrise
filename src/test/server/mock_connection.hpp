#pragma once

#include <boost/thread/future.hpp>
#include <gmock/gmock.h>
#include <base_test.hpp>
#include "server/client_connection.hpp"

namespace opossum {

class MockConnection : public ClientConnection {
 public:
  MockConnection() : ClientConnection(nullptr) {};

  MOCK_METHOD0(receive_startup_packet_header, boost::future<uint32_t>());
  MOCK_METHOD1(receive_startup_packet_contents, boost::future<void>(uint32_t size));
  MOCK_METHOD0(receive_packet_header, boost::future<RequestHeader>());
  MOCK_METHOD1(receive_packet_contents, boost::future<InputPacket>(uint32_t size));

  MOCK_METHOD0(send_ssl_denied, boost::future<void>());
  MOCK_METHOD0(send_auth, boost::future<void>());
  MOCK_METHOD0(send_ready_for_query, boost::future<void>());

};

}  // namespace opossum