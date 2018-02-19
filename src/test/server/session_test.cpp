
#include <boost/asio/ip/tcp.hpp>
#include <server/hyrise_session.hpp>
#include <gmock/gmock.h>
#include "../base_test.hpp"
#include "mock_connection.hpp"

namespace opossum {

using ::testing::Return;

class SessionTest : public BaseTest {
 protected:
  void SetUp() override {
    //init session with socket
    boost::asio::io_service io_service;
    MockConnection mockConnection;

    _mockConnection = mockConnection;
    _session = std::make_shared<HyriseSession>(socket, mockConnection);
  }
  MockConnection _mockConnection;
  std::shared_ptr<HyriseSession> _session;
};

TEST_F(SessionTest, StartSession) {
  //  EXPECT_TRUE(_session != nullptr);
  DebugAssert(_session == nullptr, "Session is nullptr");

  EXPECT_CALL(_mockConnection, receive_startup_packet_header())
              .WillOnce(Return(boost::make_ready_future((uint32_t) 100)));
  EXPECT_CALL(_mockConnection, receive_startup_packet_contents())
              .WillOnce(Return(boost::make_ready_future()));

  EXPECT_CALL(_mockConnection, send_auth())
              .Times(1);
  EXPECT_CALL(_mockConnection, send_ready_for_query())
              .Times(1);

  _session->start();

  EXPECT_TRUE(false);
}
}