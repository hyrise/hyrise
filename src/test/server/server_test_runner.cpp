#include <pqxx/pqxx>

#include <future>
#include <thread>

#include "base_test.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"

#include "server/server.hpp"

namespace opossum {

class ServerTestRunner : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().reset();
    SQLQueryCache<SQLQueryPlan>::get().clear();

    _table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", _table_a);

    // Set scheduler so that the server can execute the tasks on separate threads.
    CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());

    uint16_t server_port = 0;
    std::mutex mutex{};
    auto cv = std::make_shared<std::condition_variable>();

    auto server_runner = [&, cv](boost::asio::io_service& io_service) {
      Server server{io_service, /* port = */ 0};  // run on port 0 so the server can pick a free one

      {
        std::unique_lock<std::mutex> lock{mutex};
        server_port = server.get_port_number();
      }

      cv->notify_one();

      io_service.run();
    };

    _io_service = std::make_unique<boost::asio::io_service>();
    _server_thread = std::make_unique<std::thread>(server_runner, std::ref(*_io_service));

    // We need to wait here for the server to have started so we can get its port, which must be set != 0
    {
      std::unique_lock<std::mutex> lock{mutex};
      cv->wait(lock, [&] { return server_port != 0; });
    }

    // Get randomly assigned port number for client connection
    _connection_string = "hostaddr=127.0.0.1 port=" + std::to_string(server_port);
  }

  void TearDown() override {
    // Give the server time to shut down gracefully before force-closing the socket it's working on
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    _io_service->stop();
    _server_thread->join();
  }

  std::unique_ptr<boost::asio::io_service> _io_service;
  std::unique_ptr<std::thread> _server_thread;
  std::string _connection_string;

  std::shared_ptr<Table> _table_a;
};

TEST_F(ServerTestRunner, TestSimpleSelect) {
  pqxx::connection connection{_connection_string};

  // We use nontransactions because the regular transactions use SQL that we don't support. Nontransactions auto commit.
  pqxx::nontransaction transaction{connection};

  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), _table_a->row_count());
}

TEST_F(ServerTestRunner, TestMultipleConnections) {
  pqxx::connection connection1{_connection_string};
  pqxx::connection connection2{_connection_string};
  pqxx::connection connection3{_connection_string};

  pqxx::nontransaction transaction1{connection1};
  pqxx::nontransaction transaction2{connection2};
  pqxx::nontransaction transaction3{connection3};

  const std::string sql = "SELECT * FROM table_a;";
  const auto expected_num_rows = _table_a->row_count();

  const auto result1 = transaction1.exec(sql);
  EXPECT_EQ(result1.size(), expected_num_rows);

  const auto result2 = transaction2.exec(sql);
  EXPECT_EQ(result2.size(), expected_num_rows);

  const auto result3 = transaction3.exec(sql);
  EXPECT_EQ(result3.size(), expected_num_rows);
}

TEST_F(ServerTestRunner, TestSimpleInsertSelect) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const auto expected_num_rows = _table_a->row_count() + 1;
  transaction.exec("INSERT INTO table_a VALUES (1, 1.0);");
  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), expected_num_rows);
}

TEST_F(ServerTestRunner, TestPreparedStatement) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const std::string prepared_name = "statement1";
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");

  const auto param = 1234u;
  const auto result1 = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result1.size(), 1u);

  transaction.exec("INSERT INTO table_a VALUES (55555, 1.0);");
  const auto result2 = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result2.size(), 2u);
}

TEST_F(ServerTestRunner, TestParallelConnections) {
  // This test is by no means perfect, as it can show flaky behaviour. But it is rather hard to get reliable tests with
  // multiple concurrent connections to detect a randomly (but often) occurring bug. This test will/can only fail if a
  // bug is present but it should not fail if no bug is present. It just sends 100 parallel connections and if that
  // fails, there probably is a bug.
  const std::string sql = "SELECT * FROM table_a;";
  const auto expected_num_rows = _table_a->row_count();

  const auto connection_run = [&]() {
    pqxx::connection connection{_connection_string};
    pqxx::nontransaction transaction{connection};
    const auto result = transaction.exec(sql);
    EXPECT_EQ(result.size(), expected_num_rows);
  };

  const auto num_threads = 100u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, connection_run));
  }

  for (auto& thread_fut : thread_futures) {
    // We give this a lot of time, not because we need that long for 100 threads to finish, but because sanitizers and
    // other tools like valgrind sometimes bring a high overhead that exceeds 10 seconds.
    if (thread_fut.wait_for(std::chrono::seconds(150)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
    }
  }
}

}  // namespace opossum
