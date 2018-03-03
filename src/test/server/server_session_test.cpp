
#include <gmock/gmock.h>
#include <boost/asio/ip/tcp.hpp>
#include <server/server_session.hpp>
// The template is defined and default-instantiated in the .cpp
#include <server/server_session.cpp>
#include "../base_test.hpp"
#include "mock_connection.hpp"

namespace opossum {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::Invoke;
using ::testing::Throw;
using ::testing::ByMove;
using ::testing::NiceMock;

// We're using a NiceMock here to suppress warnings when 'uninteresting' calls happen
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

    _configure_default_message_flow();
  }

  void _configure_default_message_flow() {
    _configure_startup();
    _configure_termination();
    _configure_successful_sends();
  }

  void _configure_startup() {
    ON_CALL(*_connection, receive_startup_packet_header())
        .WillByDefault(Return(ByMove(boost::make_ready_future(uint32_t(32)))));
    ON_CALL(*_connection, receive_startup_packet_contents(_)).WillByDefault(Return(ByMove(boost::make_ready_future())));
  }

  void _configure_termination() {
    RequestHeader termination_header{NetworkMessageType::TerminateCommand, 0};
    ON_CALL(*_connection, receive_packet_header())
        .WillByDefault(Return(ByMove(boost::make_ready_future(termination_header))));
  }

  void _configure_successful_sends() {
    // When the session attempts to send something, continue normally
    // (i.e. don't throw an exception)
    ON_CALL(*_connection, send_ssl_denied()).WillByDefault(Invoke([]() { return boost::make_ready_future(); }));
    ON_CALL(*_connection, send_auth()).WillByDefault(Invoke([]() { return boost::make_ready_future(); }));
    ON_CALL(*_connection, send_ready_for_query()).WillByDefault(Invoke([]() { return boost::make_ready_future(); }));
    ON_CALL(*_connection, send_error(_))
        .WillByDefault(Invoke([](const std::string&) { return boost::make_ready_future(); }));
    ON_CALL(*_connection, send_notice(_))
        .WillByDefault(Invoke([](const std::string&) { return boost::make_ready_future(); }));
    ON_CALL(*_connection, send_status_message(_))
        .WillByDefault(Invoke([](const NetworkMessageType&) { return boost::make_ready_future(); }));
    ON_CALL(*_connection, send_row_description(_))
        .WillByDefault(Invoke([](const std::vector<ColumnDescription>&) { return boost::make_ready_future(); }));
    ON_CALL(*_connection, send_data_row(_))
        .WillByDefault(Invoke([](const std::vector<std::string>&) { return boost::make_ready_future(); }));
    ON_CALL(*_connection, send_command_complete(_))
        .WillByDefault(Invoke([](const std::string&) { return boost::make_ready_future(); }));
  }

  std::shared_ptr<TestConnection> _connection;
  std::shared_ptr<TestServerSession> _session;
};

TEST_F(ServerSessionTest, SessionPerformsStartup) {
  // Use this magic value to check if the session performs the correct calls
  uint32_t startup_packet_header_length(42);

  // This tells googlemock to check that the calls to the session are being made
  // in the same order that we specify below
  InSequence s;

  // Override the default mock implementation defined in _configure_startup by returning the magic value
  // as the header length.
  EXPECT_CALL(*_connection, receive_startup_packet_header())
      .WillOnce(Return(ByMove(boost::make_ready_future(startup_packet_header_length))));

  // Make sure receive_startup_packet_contents is called with the magic value defined above
  EXPECT_CALL(*_connection, receive_startup_packet_contents(startup_packet_header_length));

  // Expect that the session sends out an authentication response and an initial ReadyForQuery
  EXPECT_CALL(*_connection, send_auth());
  EXPECT_CALL(*_connection, send_ready_for_query());

  // Actually run the session: googlemock will record which Connection methods are called in which order
  // and with which parameters. Not all method calls need to have expectations attached, calls that are
  // uninteresting for this specific test will default to the mock implementations configured in SetUp().
  _session->start().wait();
}

TEST_F(ServerSessionTest, SessionHandlesConnectionErrorsDuringStartup) {
  InSequence s;

  EXPECT_CALL(*_connection, receive_startup_packet_header());

  auto exception = std::logic_error("Some connection problem");
  EXPECT_CALL(*_connection, receive_startup_packet_contents(_))
      .WillOnce(Return(ByMove(boost::make_exceptional_future<void>(boost::copy_exception(exception)))));

  EXPECT_NO_THROW(_session->start().wait());
}

TEST_F(ServerSessionTest, SessionDeniesSslRequestDuringStartup) {
  // 0 is what the connection reports as the header size when it receives an SSL request
  uint32_t ssl_startup_packet_header_length(0);

  InSequence s;

  EXPECT_CALL(*_connection, receive_startup_packet_header())
      .WillOnce(Return(ByMove(boost::make_ready_future(ssl_startup_packet_header_length))));
  EXPECT_CALL(*_connection, send_ssl_denied());

  EXPECT_CALL(*_connection, receive_startup_packet_header());
  EXPECT_CALL(*_connection, receive_startup_packet_contents(_));

  EXPECT_CALL(*_connection, send_auth());
  EXPECT_CALL(*_connection, send_ready_for_query());

  _session->start().wait();
}

TEST_F(ServerSessionTest, SessionShutsDownOnTerminationPacket) {
  InSequence s;

  EXPECT_CALL(*_connection, send_ready_for_query());

  // Expect that receive_packet_header is only called once (implying that the session is closed after
  // receiving the termination packet set in _configure_termination)
  EXPECT_CALL(*_connection, receive_packet_header());

  _session->start().wait();
}

TEST_F(ServerSessionTest, SessionShutsDownOnErrorReceivingPacketHeader) {
  InSequence s;

  EXPECT_CALL(*_connection, send_ready_for_query());

  auto exception = std::logic_error("Some connection problem");
  EXPECT_CALL(*_connection, receive_packet_header())
      .WillOnce(Return(ByMove(boost::make_exceptional_future<RequestHeader>(boost::copy_exception(exception)))));

  EXPECT_NO_THROW(_session->start().wait());
}

TEST_F(ServerSessionTest, SessionRecoversFromErrorsDuringCommandProcessing) {
  InSequence s;

  EXPECT_CALL(*_connection, send_ready_for_query());

  RequestHeader request{NetworkMessageType::SimpleQueryCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(request))));

  auto exception = std::logic_error("Some connection problem");
  EXPECT_CALL(*_connection, receive_packet_contents(42))
      .WillOnce(Return(ByMove(boost::make_exceptional_future<InputPacket>(boost::copy_exception(exception)))));

  // Expect that the session sends an error packet to the client,
  // containing the exception's message
  EXPECT_CALL(*_connection, send_error(exception.what()));

  // Expect that the session tells the client to continue with the next command
  EXPECT_CALL(*_connection, send_ready_for_query());
  EXPECT_CALL(*_connection, receive_packet_header());

  _session->start().wait();
}

}  // namespace opossum
