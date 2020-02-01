#include <pqxx/pqxx>

#include <future>
#include <thread>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_plan_cache.hpp"

#include "server/server.hpp"

namespace opossum {

// This class tests supported operations of the server implementation. This does not include statements with named
// portals which are used for CURSOR operations.
class ServerTestRunner : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::reset();

    _table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_a", _table_a);

    // Set scheduler so that the server can execute the tasks on separate threads.
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

    auto server_runner = [](Server& server) { server.run(); };

    _server_thread = std::make_unique<std::thread>(server_runner, std::ref(*_server));

    // Get randomly assigned port number for client connection
    _connection_string = "hostaddr=127.0.0.1 port=" + std::to_string(_server->server_port());
  }

  void TearDown() override {
    _server->shutdown();

    // Give the server time to shut down gracefully before force-closing the socket it's working on
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    _server_thread->join();
  }

  std::unique_ptr<Server> _server = std::make_unique<Server>(
      boost::asio::ip::address(), 0, SendExecutionInfo::No);  // Port 0 to select random open port
  std::unique_ptr<std::thread> _server_thread;
  std::string _connection_string;

  std::shared_ptr<Table> _table_a;
};

TEST_F(ServerTestRunner, TestSimpleSelect) {
  pqxx::connection connection{_connection_string};

  // We use nontransactions because the regular transactions use "begin" and "commit" keywords that we do not support.
  // Nontransactions auto commit.
  pqxx::nontransaction transaction{connection};

  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), _table_a->row_count());
}

TEST_F(ServerTestRunner, ValidateCorrectTransfer) {
  const auto all_types_table = load_table("resources/test_data/tbl/all_data_types_sorted.tbl", 2);
  Hyrise::get().storage_manager.add_table("all_types_table", all_types_table);

  pqxx::connection connection{_connection_string};

  // We use nontransactions because the regular transactions use SQL that we do not support. Nontransactions auto
  // commit.
  pqxx::nontransaction transaction{connection};

  const auto result = transaction.exec("SELECT * FROM all_types_table;");

  EXPECT_EQ(result.size(), all_types_table->row_count());
  EXPECT_EQ(result[0].size(), static_cast<size_t>(all_types_table->column_count()));

  for (uint64_t row_id = 0; row_id < all_types_table->row_count(); row_id++) {
    const auto current_row = all_types_table->get_row(row_id);
    for (ColumnID column_count{0}; column_count < all_types_table->column_count(); column_count++) {
      // Representation of NULL values in pqxx::field differ from result of lexical_cast
      if (result[row_id][column_count].is_null()) {
        EXPECT_EQ("NULL", boost::lexical_cast<std::string>(current_row[column_count]));
      } else {
        EXPECT_EQ(result[row_id][column_count].c_str(), boost::lexical_cast<std::string>(current_row[column_count]));
      }
    }
  }
}

TEST_F(ServerTestRunner, TestCopyImport) {
  pqxx::connection connection{_connection_string};

  // We use nontransactions because the regular transactions use "begin" and "commit" keywords that we do not support.
  // Nontransactions auto commit.
  pqxx::nontransaction transaction{connection};

  const auto result = transaction.exec("COPY another_table FROM 'resources/test_data/tbl/int_float.tbl';");

  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("another_table"));
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("another_table"), _table_a);
}

TEST_F(ServerTestRunner, TestInvalidStatement) {
  pqxx::connection connection{_connection_string};

  // We use nontransactions because the regular transactions use SQL that we do not support. Nontransactions auto
  // commit.
  pqxx::nontransaction transaction{connection};

  // Ill-formed SQL statement
  EXPECT_THROW(transaction.exec("SELECT * FROM;"), pqxx::sql_error);

  // Well-formed but table does not exist
  EXPECT_THROW(transaction.exec("SELECT * FROM non_existent;"), pqxx::sql_error);

  // Check whether server is still running and connection established
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
  const auto result2 = transaction.exec_prepared(prepared_name, 123);
  EXPECT_EQ(result2.size(), 2u);

  transaction.exec("INSERT INTO table_a VALUES (55555, 1.0);");
  const auto result3 = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result3.size(), 2u);
}

TEST_F(ServerTestRunner, TestUnnamedPreparedStatement) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const std::string prepared_name = "";
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");

  const auto param = 1234u;
  const auto result1 = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result1.size(), 1u);

  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a <= ?");

  const auto result2 = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result2.size(), 2u);
}

TEST_F(ServerTestRunner, TestInvalidPreparedStatement) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const std::string prepared_name = "";
  const auto param = 1234u;

  // Ill-formed prepared statement
  EXPECT_THROW(connection.prepare(prepared_name, "SELECT * FROM WHERE a > ?"), pqxx::sql_error);

  // Well-formed but table does not exist
  EXPECT_ANY_THROW(connection.prepare(prepared_name, "SELECT * FROM non_existent WHERE a > ?"));

  // Wrong number of parameters
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ? and a > ?");
  EXPECT_ANY_THROW(transaction.exec_prepared(prepared_name, param));

  // Check whether server is still running and connection established
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");
  const auto result = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result.size(), 1u);
}

TEST_F(ServerTestRunner, TestParallelConnections) {
  // This test is by no means perfect, as it can show flaky behaviour. But it is rather hard to get reliable tests with
  // multiple concurrent connections to detect a randomly (but often) occurring bug. This test will/can only fail if a
  // bug is present but it should not fail if no bug is present. It just sends 100 parallel connections and if that
  // fails, there probably is a bug.
  const std::string sql = "SELECT * FROM table_a;";
  const auto expected_num_rows = _table_a->row_count();

  // Define the work package
  const auto connection_run = [&]() {
    pqxx::connection connection{_connection_string};
    pqxx::nontransaction transaction{connection};
    const auto result = transaction.exec(sql);
    EXPECT_EQ(result.size(), expected_num_rows);
  };

  // Create the async objects and spawn them asynchronously (i.e., as their own threads)
  const auto num_threads = 100u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, connection_run));
  }

  // Wait for completion or timeout (should not occur)
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we need that long for 100 threads to finish, but because sanitizers and
    // other tools like valgrind sometimes bring a high overhead that exceeds 10 seconds.
    if (thread_future.wait_for(std::chrono::seconds(150)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
      // Retrieve the future so that exceptions stored in its state are thrown
      thread_future.get();
    }
  }
}

TEST_F(ServerTestRunner, TestTransactionConflicts) {
  // Similar to TestParallelConnections, but this time we modify the table, expecting some conflicts on the way
  // Also similar to StressTest.TestTransactionConflicts, only that we go through the server
  auto initial_sum = int64_t{};
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, table] = pipeline.get_result_table();
    initial_sum = table->get_value<int64_t>(ColumnID{0}, 0);
  }

  std::atomic_int successful_increments{0};
  std::atomic_int conflicted_increments{0};
  const auto iterations_per_thread = 10;

  // Define the work package
  const auto connection_run = [&]() {
    for (auto iteration = 0; iteration < iterations_per_thread; ++iteration) {
      pqxx::connection connection{_connection_string};
      pqxx::nontransaction transaction{connection};
      try {
        const std::string sql = "UPDATE table_a SET a = a + 1 WHERE a = (SELECT MIN(a) FROM table_a);";
        transaction.exec(sql);
        ++successful_increments;
      } catch (const pqxx::serialization_failure&) {
        // As expected, some of these updates fail
        ++conflicted_increments;
      }
    }
  };

  // Create the async objects and spawn them asynchronously (i.e., as their own threads)
  const auto num_threads = 100u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, connection_run));
  }

  // Wait for completion or timeout (should not occur)
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we need that long for 100 threads to finish, but because sanitizers and
    // other tools like valgrind sometimes bring a high overhead that exceeds 10 seconds.
    if (thread_future.wait_for(std::chrono::seconds(150)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
      // Retrieve the future so that exceptions stored in its state are thrown
      thread_future.get();
    }
  }

  // Verify results
  auto final_sum = int64_t{};
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, table] = pipeline.get_result_table();
    final_sum = table->get_value<int64_t>(ColumnID{0}, 0);
  }

  // Really pessimistic, but at least 2 statements should have made it
  EXPECT_GT(successful_increments, 2);

  // We also want to see at least one conflict so that we can be sure that conflict handling in the server works.
  // In case this fails, start playing with num_threads to provoke more conflicts.
  EXPECT_GE(conflicted_increments, 1);

  EXPECT_EQ(successful_increments + conflicted_increments, num_threads * iterations_per_thread);
  EXPECT_FLOAT_EQ(final_sum - initial_sum, successful_increments);
}

}  // namespace opossum
