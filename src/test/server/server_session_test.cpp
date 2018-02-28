
#include <boost/asio/ip/tcp.hpp>
#include <server/server_session.hpp>
#include <gmock/gmock.h>
// The template is defined and default-instantiated in the .cpp
#include <server/server_session.cpp>
#include "../base_test.hpp"
#include "mock_connection.hpp"

namespace opossum {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::ByMove;
using ::testing::NiceMock;

// We're using a NiceMock here to suppress warnings when "uninteresting" calls happen
// (i.e. calls irrelevant to the specific test case, defaulting to a mock specified using ON_CALL().WillByDefault() )
// https://github.com/google/googlemock/blob/master/googlemock/docs/CookBook.md#the-nice-the-strict-and-the-naggy
using TestConnection = NiceMock<MockConnection>;
using TestServerSession = ServerSessionImpl<TestConnection>;

class ServerSessionTest : public BaseTest {
 protected:
  void SetUp() override {
    // The io_service is not run during the tests. It is currently required by the Session
    // to dispatch task results to the main thread -- if this functionality was encapsulated
    // in an injected object similar to the (Mock)Connection, we'd gain additional capabilities for testing
    boost::asio::io_service io_service;

    _connection = std::make_shared<TestConnection>();
    _session = std::make_shared<TestServerSession>(io_service, _connection);
  }

  void configure_default_message_flow() {
    configure_startup();
    configure_termination();
    configure_successful_sends();
  }

  void configure_startup() {
    ON_CALL(*_connection, receive_startup_packet_header())
        .WillByDefault(Return(ByMove(boost::make_ready_future(uint32_t(32)))));
    ON_CALL(*_connection, receive_startup_packet_contents(_)).WillByDefault(Return(ByMove(boost::make_ready_future())));
  }

  void configure_termination() {
    RequestHeader termination_header{NetworkMessageType::TerminateCommand, 0};
    ON_CALL(*_connection, receive_packet_header())
        .WillByDefault(Return(ByMove(boost::make_ready_future(termination_header))));
  }

  void configure_successful_sends() {
    // When the session attempts to send something, continue normally
    // (i.e. don't throw an exception)
    ON_CALL(*_connection, send_ssl_denied()).WillByDefault(Return(ByMove(boost::make_ready_future())));
    ON_CALL(*_connection, send_auth()).WillByDefault(Return(ByMove(boost::make_ready_future())));
    ON_CALL(*_connection, send_ready_for_query()).WillByDefault(Return(ByMove(boost::make_ready_future())));
    ON_CALL(*_connection, send_error(_)).WillByDefault(Return(ByMove(boost::make_ready_future())));
    ON_CALL(*_connection, send_notice(_)).WillByDefault(Return(ByMove(boost::make_ready_future())));
    ON_CALL(*_connection, send_status_message(_)).WillByDefault(Return(ByMove(boost::make_ready_future())));
    ON_CALL(*_connection, send_row_description(_)).WillByDefault(Return(ByMove(boost::make_ready_future())));
    ON_CALL(*_connection, send_data_row(_)).WillByDefault(Return(ByMove(boost::make_ready_future())));
    ON_CALL(*_connection, send_command_complete(_)).WillByDefault(Return(ByMove(boost::make_ready_future())));
  }

  std::shared_ptr<TestConnection> _connection;
  std::shared_ptr<TestServerSession> _session;
};

TEST_F(ServerSessionTest, StartServerSession) {
  configure_default_message_flow();

  uint32_t startup_packet_header_length(42);

  // Make expectations
  InSequence s;

  // Make sure receive_startup_packet_contents is called with the 42 defined above
  EXPECT_CALL(*_connection, receive_startup_packet_header())
      .Times(1)
      .WillOnce(Return(ByMove(boost::make_ready_future(startup_packet_header_length))));
  EXPECT_CALL(*_connection, receive_startup_packet_contents(startup_packet_header_length)).Times(1);

  EXPECT_CALL(*_connection, send_auth()).Times(1);
  EXPECT_CALL(*_connection, send_ready_for_query()).Times(1);

  _session->start().wait();
}

TEST_F(ServerSessionTest, StartServerSessionWithSslRequest) {
  configure_default_message_flow();

  // 0 is what the connection reports as the header size when it receives an SSL request
  uint32_t ssl_startup_packet_header_length(0);

  InSequence s;

  EXPECT_CALL(*_connection, receive_startup_packet_header())
      .Times(1)
      .WillOnce(Return(ByMove(boost::make_ready_future(ssl_startup_packet_header_length))));
  EXPECT_CALL(*_connection, send_ssl_denied()).Times(1);

  EXPECT_CALL(*_connection, receive_startup_packet_header()).Times(1);
  EXPECT_CALL(*_connection, receive_startup_packet_contents(_)).Times(1);

  EXPECT_CALL(*_connection, send_auth()).Times(1);
  EXPECT_CALL(*_connection, send_ready_for_query()).Times(1);

  _session->start().wait();
}

}  // namespace opossum
