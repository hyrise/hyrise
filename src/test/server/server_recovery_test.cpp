#include <pqxx/pqxx>

#include <thread>

#include "base_test.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"

#include "server/server.hpp"
#include "concurrency/logging/logger.hpp"

namespace opossum {

class ServerRecoveryTest : public BaseTestWithParam<Logger::Implementation> {
 protected:

  void start_server() {
    StorageManager::get().reset();
    SQLQueryCache<SQLQueryPlan>::get().clear();

    // just to be sure, since it is essential for these tests
    EXPECT_FALSE(StorageManager::get().has_table("a_table"));

    // _table_a = load_table("src/test/tables/int_float.tbl", 2);
    // StorageManager::get().add_table("table_a", _table_a);

    // Set scheduler so that the server can execute the tasks on separate threads.
    CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_numa_topology()));

    uint16_t server_port = 0;
    std::mutex mutex{};
    std::condition_variable cv{};

    auto server_runner = [&](boost::asio::io_service& io_service) {
      Server server{io_service, /* port = */ 0};  // run on port 0 so the server can pick a free one

      {
        std::unique_lock<std::mutex> lock{mutex};
        server_port = server.get_port_number();
      }

      cv.notify_one();

      io_service.run();
    };

    _io_service = std::make_unique<boost::asio::io_service>();
    _server_thread = std::make_unique<std::thread>(server_runner, std::ref(*_io_service));

    // We need to wait here for the server to have started so we can get its port, which must be set != 0
    {
      std::unique_lock<std::mutex> lock{mutex};
      cv.wait(lock, [&] { return server_port != 0; });
    }

    // Get randomly assigned port number for client connection
    _connection_string = "hostaddr=127.0.0.1 port=" + std::to_string(server_port);
  }

  void SetUp() override {
    start_server();
  }

  void shutdown_server() {
    // Give the server time to shut down gracefully before force-closing the socket it's working on
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    _io_service->stop();
    _server_thread->join();
  }

  void TearDown() override {
    shutdown_server();
    Logger::delete_log_files();
  }

  void restart_server() {
    shutdown_server();
    start_server();
  }

  std::unique_ptr<boost::asio::io_service> _io_service;
  std::unique_ptr<std::thread> _server_thread;
  std::string _connection_string;

  // std::shared_ptr<Table> _table_a;
};

// TEST_F(ServerRecoveryTest, TestSimpleSelect) {
//   pqxx::connection connection{_connection_string};

//   // We use nontransactions because the regular transactions use SQL that we don't support. Nontransactions auto commit.
//   pqxx::nontransaction transaction{connection};

//   const auto result = transaction.exec("SELECT * FROM table_a;");
//   EXPECT_EQ(result.size(), _table_a->row_count());
// }

// TEST_F(ServerRecoveryTest, TestMultipleConnections) {
//   pqxx::connection connection1{_connection_string};
//   pqxx::connection connection2{_connection_string};
//   pqxx::connection connection3{_connection_string};

//   pqxx::nontransaction transaction1{connection1};
//   pqxx::nontransaction transaction2{connection2};
//   pqxx::nontransaction transaction3{connection3};

//   const std::string sql = "SELECT * FROM table_a;";
//   const auto expected_num_rows = _table_a->row_count();

//   const auto result1 = transaction1.exec(sql);
//   EXPECT_EQ(result1.size(), expected_num_rows);

//   const auto result2 = transaction2.exec(sql);
//   EXPECT_EQ(result2.size(), expected_num_rows);

//   const auto result3 = transaction3.exec(sql);
//   EXPECT_EQ(result3.size(), expected_num_rows);
// }

TEST_P(ServerRecoveryTest, TestSimpleInsert) {
  Logger::set_implementation(GetParam());

  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  auto a_table = load_table("src/test/tables/int_float.tbl", 2);

  transaction.exec("load src/test/tables/int_float.tbl a_table;");
  const auto result = transaction.exec("SELECT * FROM a_table;");
  EXPECT_EQ(result.size(), a_table->row_count());

  transaction.exec("INSERT INTO a_table VALUES (1, 1.0);");
  transaction.exec("INSERT INTO a_table VALUES (994, 994.0);");

  // // const auto expected_num_rows = _table_a->row_count() + 1;
  // transaction.exec("INSERT INTO int_float_table VALUES (1, 1.0);");
  // transaction.exec("INSERT INTO int_float_table VALUES (994, 994.0);");
  // const auto result = transaction.exec("SELECT * FROM int_float_table;");
  // EXPECT_EQ(result.size(), 4u);

  shutdown_server();

  start_server();
  
  pqxx::connection connection2{_connection_string};
  pqxx::nontransaction transaction2{connection2};
  const auto result2 = transaction2.exec("SELECT * FROM a_table;");
  EXPECT_EQ(result2.size(), a_table->row_count() + 2);

  // clean logs

  EXPECT_EQ(true, true);
}

// TEST_F(ServerRecoveryTest, TestPreparedStatement) {
//   pqxx::connection connection{_connection_string};
//   pqxx::nontransaction transaction{connection};

//   const std::string prepared_name = "statement1";
//   connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");

//   const auto param = 1234u;
//   const auto result1 = transaction.exec_prepared(prepared_name, param);
//   EXPECT_EQ(result1.size(), 1u);

//   transaction.exec("INSERT INTO table_a VALUES (55555, 1.0);");
//   const auto result2 = transaction.exec_prepared(prepared_name, param);
//   EXPECT_EQ(result2.size(), 2u);
// }

// const JoinDetectionTestParam test_queries[] = {{__LINE__, "SELECT * FROM a, b WHERE a.a = b.a", 1},
//                                                {__LINE__, "SELECT * FROM a, b, c WHERE a.a = c.a", 1},
//                                                {__LINE__, "SELECT * FROM a, b, c WHERE b.a = c.a", 1}};

// auto formatter = [](const testing::TestParamInfo<struct JoinDetectionTestParam> info) {
//   return std::to_string(info.param.line);
// };
// INSTANTIATE_TEST_CASE_P(test_queries, JoinDetectionRuleTest, ::testing::ValuesIn(test_queries), formatter);


Logger::Implementation logging_implementations[] = {
  Logger::Implementation::Simple, 
  Logger::Implementation::GroupCommit
};

auto formatter = [](const testing::TestParamInfo<Logger::Implementation> info) {
  switch (info.param) {
    case Logger::Implementation::Simple: return "Simple";
    case Logger::Implementation::GroupCommit: return "GroupCommit";
    default: return "unknown";
  }
};

INSTANTIATE_TEST_CASE_P(logging_implementations, ServerRecoveryTest, ::testing::ValuesIn(logging_implementations), formatter);

}  // namespace opossum
