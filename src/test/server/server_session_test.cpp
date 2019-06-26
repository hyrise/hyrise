#include <gmock/gmock.h>
#include <boost/asio/ip/tcp.hpp>
#include <server/server_session.hpp>
// The template is ServerSessionImpl defined and default-instantiated in the .cpp, we include it here to mock it
#include <server/server_session.cpp>  // NOLINT
#include "base_test.hpp"
#include "mock_connection.hpp"
#include "mock_task_runner.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

using ::testing::_;
using ::testing::An;
using ::testing::ByMove;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::Throw;

// We're using a NiceMock here to suppress warnings when 'uninteresting' calls happen
// (i.e. calls irrelevant to the specific test case, defaulting to a mock specified using ON_CALL().WillByDefault() )
// https://github.com/google/googlemock/blob/master/googlemock/docs/CookBook.md#the-nice-the-strict-and-the-naggy
using TestConnection = NiceMock<MockConnection>;
using TestTaskRunner = NiceMock<MockTaskRunner>;
using TestServerSession = ServerSessionImpl<TestConnection, TestTaskRunner>;

class ServerSessionTest : public BaseTest {
 protected:
  void SetUp() override {
    _connection = std::make_shared<TestConnection>();
    _task_runner = std::make_shared<TestTaskRunner>();

    _session = std::make_shared<TestServerSession>(_connection, _task_runner);

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
    ON_CALL(*_connection, receive_startup_packet_body(_)).WillByDefault(Return(ByMove(boost::make_ready_future())));
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
    ON_CALL(*_connection, send_parameter_status(_, _)).WillByDefault(Invoke([](const std::string&, const std::string&) {
      return boost::make_ready_future();
    }));
    ON_CALL(*_connection, send_ready_for_query()).WillByDefault(Invoke([]() { return boost::make_ready_future(); }));
    ON_CALL(*_connection, send_error(_)).WillByDefault(Invoke([](const std::string&) {
      return boost::make_ready_future();
    }));
    ON_CALL(*_connection, send_notice(_)).WillByDefault(Invoke([](const std::string&) {
      return boost::make_ready_future();
    }));
    ON_CALL(*_connection, send_status_message(_)).WillByDefault(Invoke([](const NetworkMessageType&) {
      return boost::make_ready_future();
    }));
    ON_CALL(*_connection, send_row_description(_)).WillByDefault(Invoke([](const std::vector<ColumnDescription>&) {
      return boost::make_ready_future();
    }));
    ON_CALL(*_connection, send_data_row(_)).WillByDefault(Invoke([](const std::vector<std::string>&) {
      return boost::make_ready_future();
    }));
    ON_CALL(*_connection, send_command_complete(_)).WillByDefault(Invoke([](const std::string&) {
      return boost::make_ready_future();
    }));
  }

  std::shared_ptr<SQLPipeline> _create_working_sql_pipeline() {
    // We don't mock the SQL Pipeline, so we have to provide a query that executes successfully
    auto t = load_table("resources/test_data/tbl/int.tbl", 10);
    Hyrise::get().storage_manager.add_table("foo", t);
    return std::make_shared<SQLPipeline>(SQLPipelineBuilder{"SELECT * FROM foo;"}.create_pipeline());
  }

  std::shared_ptr<TestConnection> _connection;
  std::shared_ptr<TestTaskRunner> _task_runner;
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

  // Make sure receive_startup_packet_body is called with the magic value defined above
  EXPECT_CALL(*_connection, receive_startup_packet_body(startup_packet_header_length));

  // Expect that the session sends out an authentication response and an initial ReadyForQuery
  EXPECT_CALL(*_connection, send_auth());
  EXPECT_CALL(*_connection, send_parameter_status(_, _)).Times(2);
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
  EXPECT_CALL(*_connection, receive_startup_packet_body(_))
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
  EXPECT_CALL(*_connection, receive_startup_packet_body(_));

  EXPECT_CALL(*_connection, send_auth());
  EXPECT_CALL(*_connection, send_parameter_status(_, _)).Times(2);
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
  EXPECT_CALL(*_connection, receive_simple_query_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_exceptional_future<std::string>(boost::copy_exception(exception)))));

  // Expect that the session sends an error packet to the client,
  // containing the exception's message
  EXPECT_CALL(*_connection, send_error(exception.what()));

  // Expect that the session tells the client to continue with the next command
  EXPECT_CALL(*_connection, send_ready_for_query());
  EXPECT_CALL(*_connection, receive_packet_header());

  _session->start().wait();
}

TEST_F(ServerSessionTest, SessionExecutesSimpleQueryCommand) {
  InSequence s;

  // The session initiates with a ReadyForQuery message
  EXPECT_CALL(*_connection, send_ready_for_query());

  // The connection sends the appropriate packet header...
  RequestHeader request{NetworkMessageType::SimpleQueryCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(request))));

  // ... as well as the SQL
  EXPECT_CALL(*_connection, receive_simple_query_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(std::string("SELECT * FROM foo;")))));

  // The session creates a SQLPipeline using a scheduled task (we're providing a 'real' SQLPipeline in the result)
  auto create_pipeline_result = std::make_unique<CreatePipelineResult>();
  create_pipeline_result->sql_pipeline = _create_working_sql_pipeline();
  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<CreatePipelineTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future(std::move(create_pipeline_result)))));

  // The session executes the SQLPipeline using another scheduled task
  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<ExecuteServerQueryTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future())));

  // It sends the result schema...
  EXPECT_CALL(*_connection, send_row_description(_));

  // ... as well as the row data (one message per row)
  EXPECT_CALL(*_connection, send_data_row(_)).Times(3);

  // Finally, the session completes the command...
  EXPECT_CALL(*_connection, send_command_complete(_));

  // sends some execution statistics...
  EXPECT_CALL(*_connection, send_notice(_));

  // and accepts the next query
  EXPECT_CALL(*_connection, send_ready_for_query());
  EXPECT_CALL(*_connection, receive_packet_header());

  _session->start().wait();
}

TEST_F(ServerSessionTest, SessionHandlesExtendedProtocolFlow) {
  InSequence s;

  // The session initiates with a ReadyForQuery message
  EXPECT_CALL(*_connection, send_ready_for_query());

  // We start with a Parse command, containing the SQL
  RequestHeader parse_request{NetworkMessageType::ParseCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(parse_request))));

  ParsePacket parse_packet = {"", "SELECT * FROM foo;"};
  EXPECT_CALL(*_connection, receive_parse_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(parse_packet))));

  // The session creates a SQLPipeline using a scheduled task (we're providing a 'real' SQLPipeline in the result)
  auto sql_pipeline = _create_working_sql_pipeline();
  auto parse_server_prepared_plan_result =
      std::make_unique<PreparedPlan>(sql_pipeline->get_optimized_logical_plans().front(), std::vector<ParameterID>{});
  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<ParseServerPreparedStatementTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future(std::move(parse_server_prepared_plan_result)))));

  EXPECT_CALL(*_connection, send_status_message(NetworkMessageType::ParseComplete));

  // Up next is a Bind command
  RequestHeader bind_request{NetworkMessageType::BindCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(bind_request))));

  BindPacket bind_packet = {"", "", {}};
  EXPECT_CALL(*_connection, receive_bind_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(bind_packet))));

  // The session schedules a task to derive a Query Plan from the SQL Pipeline
  const auto placeholder_plan = sql_pipeline->get_physical_plans().front();

  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<BindServerPreparedStatementTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future(placeholder_plan->deep_copy()))));

  EXPECT_CALL(*_connection, send_status_message(NetworkMessageType::BindComplete));

  // Next: Execute command
  RequestHeader execute_request{NetworkMessageType::ExecuteCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header())
      .WillOnce(Return(ByMove(boost::make_ready_future(execute_request))));

  EXPECT_CALL(*_connection, receive_execute_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(std::string("")))));

  // The session executes the SQLPipeline using another scheduled task
  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<ExecuteServerPreparedStatementTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future(sql_pipeline->get_result_table().second))));

  // It sends the row data (one message per row)
  EXPECT_CALL(*_connection, send_data_row(_)).Times(3);

  // ... and completes the command
  EXPECT_CALL(*_connection, send_command_complete(_));

  // Finally: Sync command
  RequestHeader sync_request{NetworkMessageType::SyncCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(sync_request))));

  EXPECT_CALL(*_connection, receive_sync_packet_body(42)).WillOnce(Return(ByMove(boost::make_ready_future())));

  EXPECT_CALL(*_connection, send_ready_for_query());

  // Session accepts the next query
  EXPECT_CALL(*_connection, receive_packet_header());

  _session->start().wait();
}

TEST_F(ServerSessionTest, SessionHandlesLoadTableRequestInSimpleQueryCommand) {
  InSequence s;

  EXPECT_CALL(*_connection, send_ready_for_query());

  RequestHeader request{NetworkMessageType::SimpleQueryCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(request))));

  EXPECT_CALL(*_connection, receive_simple_query_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(std::string("<some load table command>")))));

  // The session schedules a CreatePipelineTask which is responsible for detecting the load table request
  auto create_pipeline_result = std::make_unique<CreatePipelineResult>();
  create_pipeline_result->load_table = std::make_pair("file name", "table name");
  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<CreatePipelineTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future(std::move(create_pipeline_result)))));

  // The session schedules a LoadServerFileTask
  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<LoadServerFileTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future())));

  EXPECT_CALL(*_connection, send_ready_for_query());
  EXPECT_CALL(*_connection, receive_packet_header());

  _session->start().wait();
}

TEST_F(ServerSessionTest, SessionSendsErrorWhenRedefiningNamedStatement) {
  InSequence s;

  EXPECT_CALL(*_connection, send_ready_for_query());

  RequestHeader parse_request{NetworkMessageType::ParseCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(parse_request))));

  ParsePacket parse_packet = {"my_named_statement", "SELECT * FROM foo;"};
  EXPECT_CALL(*_connection, receive_parse_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(parse_packet))));

  // For this test, we don't actually have to set the SQL Pipeline in the result
  auto sql_pipeline = _create_working_sql_pipeline();
  auto parse_server_prepared_plan_result =
      std::make_unique<PreparedPlan>(sql_pipeline->get_optimized_logical_plans().front(), std::vector<ParameterID>{});
  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<ParseServerPreparedStatementTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future(std::move(parse_server_prepared_plan_result)))));

  EXPECT_CALL(*_connection, send_status_message(NetworkMessageType::ParseComplete));

  // Send the same parse packet again
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(parse_request))));
  EXPECT_CALL(*_connection, receive_parse_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(parse_packet))));

  EXPECT_CALL(
      *_connection,
      send_error(
          "Invalid input error: Named prepared statements must be explicitly closed before they can be redefined."));

  EXPECT_CALL(*_connection, send_ready_for_query());
  EXPECT_CALL(*_connection, receive_packet_header());

  _session->start().wait();
}

TEST_F(ServerSessionTest, SessionSendsErrorWhenBindingUnknownNamedStatement) {
  InSequence s;

  EXPECT_CALL(*_connection, send_ready_for_query());

  RequestHeader bind_request{NetworkMessageType::BindCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(bind_request))));

  BindPacket bind_packet = {"my_named_statement", "", {}};
  EXPECT_CALL(*_connection, receive_bind_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(bind_packet))));

  EXPECT_CALL(*_connection, send_error("Invalid input error: The specified statement does not exist."));

  EXPECT_CALL(*_connection, send_ready_for_query());
  EXPECT_CALL(*_connection, receive_packet_header());

  _session->start().wait();
}

TEST_F(ServerSessionTest, SessionSendsErrorWhenRedefiningNamedPortal) {
  InSequence s;

  // Run the initial workflow of the SessionHandlesExtendedProtocolFlow test
  EXPECT_CALL(*_connection, send_ready_for_query());

  // Create a statement to bind
  RequestHeader parse_request{NetworkMessageType::ParseCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(parse_request))));

  ParsePacket parse_packet = {"my_named_statement", "SELECT * FROM foo;"};
  EXPECT_CALL(*_connection, receive_parse_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(parse_packet))));

  auto sql_pipeline = _create_working_sql_pipeline();
  auto parse_server_prepared_plan_result =
      std::make_unique<PreparedPlan>(sql_pipeline->get_optimized_logical_plans().front(), std::vector<ParameterID>{});
  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<ParseServerPreparedStatementTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future(std::move(parse_server_prepared_plan_result)))));

  EXPECT_CALL(*_connection, send_status_message(NetworkMessageType::ParseComplete));

  // The first Bind command
  RequestHeader bind_request{NetworkMessageType::BindCommand, 42};
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(bind_request))));

  BindPacket bind_packet = {"my_named_statement", "my_named_portal", {}};
  EXPECT_CALL(*_connection, receive_bind_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(bind_packet))));

  const auto placeholder_plan = sql_pipeline->get_physical_plans().front();

  EXPECT_CALL(*_task_runner, dispatch_server_task(An<std::shared_ptr<BindServerPreparedStatementTask>>()))
      .WillOnce(Return(ByMove(boost::make_ready_future(placeholder_plan->deep_copy()))));

  EXPECT_CALL(*_connection, send_status_message(NetworkMessageType::BindComplete));

  // Send the same Bind command again
  EXPECT_CALL(*_connection, receive_packet_header()).WillOnce(Return(ByMove(boost::make_ready_future(bind_request))));
  EXPECT_CALL(*_connection, receive_bind_packet_body(42))
      .WillOnce(Return(ByMove(boost::make_ready_future(bind_packet))));

  EXPECT_CALL(*_connection,
              send_error("Invalid input error: Named portals must be explicitly closed before they can be redefined."));

  EXPECT_CALL(*_connection, send_ready_for_query());
  EXPECT_CALL(*_connection, receive_packet_header());

  _session->start().wait();
}

}  // namespace opossum
