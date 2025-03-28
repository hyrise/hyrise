#include <fstream>
#include <future>
#include <thread>

// GCC in release mode finds potentially uninitialized memory in pqxx. Looking at param.hxx, this appears to be a false
// positive.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <pqxx/connection>      // NOLINT(build/include_order): cpplint considers pqxx as C system headers.
#include <pqxx/nontransaction>  // NOLINT(build/include_order)
#pragma GCC diagnostic pop

#include "base_test.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/validate.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "server/server.hpp"
#include "sql/sql_plan_cache.hpp"

namespace hyrise {

// This class tests supported operations of the server implementation. This does not include statements with named
// portals which are used for CURSOR operations.
class ServerTestRunner : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
    Hyrise::get().storage_manager.add_table("table_a", _table_a);

    auto server_runner = [](Server& server) {
      server.run();
    };

    _server_thread = std::make_unique<std::thread>(server_runner, std::ref(*_server));

    // Get randomly assigned port number for client connection
    _connection_string = "hostaddr=127.0.0.1 port=" + std::to_string(_server->server_port());
    std::remove((_export_filename + ".bin").c_str());
    std::remove((_export_filename + ".csv").c_str());
    std::remove((_export_filename + ".csv.json").c_str());

    // Wait to run the server and set the scheduler
    while (!_server->is_initialized()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  void TearDown() override {
    _server->shutdown();

    // Give the server time to shut down gracefully before force-closing the socket it's working on
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    _server_thread->join();

    std::remove((_export_filename + ".bin").c_str());
    std::remove((_export_filename + ".csv").c_str());
    std::remove((_export_filename + ".csv.json").c_str());
  }

  std::unique_ptr<Server> _server = std::make_unique<Server>(
      boost::asio::ip::address(), 0, SendExecutionInfo::No);  // Port 0 to select random open port
  std::unique_ptr<std::thread> _server_thread;
  std::string _connection_string;

  std::shared_ptr<Table> _table_a;
  const std::string _export_filename = test_data_path + "server_test";
};

TEST_F(ServerTestRunner, TestCacheAndSchedulerInitialization) {
  EXPECT_NE(std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler()), nullptr);
  EXPECT_NE(Hyrise::get().default_lqp_cache, nullptr);
  EXPECT_NE(Hyrise::get().default_pqp_cache, nullptr);
}

TEST_F(ServerTestRunner, TestSimpleSelect) {
  auto connection = pqxx::connection{_connection_string};

  // We use nontransactions because the regular transactions use "begin" and "commit" keywords that we do not support.
  // Nontransactions auto commit.
  auto transaction = pqxx::nontransaction{connection};

  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), _table_a->row_count());
}

TEST_F(ServerTestRunner, ValidateCorrectTransfer) {
  const auto all_types_table = load_table("resources/test_data/tbl/all_data_types_sorted.tbl", ChunkOffset{2});
  Hyrise::get().storage_manager.add_table("all_types_table", all_types_table);

  auto connection = pqxx::connection{_connection_string};

  // We use nontransactions because the regular transactions use SQL that we do not support. Nontransactions auto
  // commit.
  auto transaction = pqxx::nontransaction{connection};

  const auto result = transaction.exec("SELECT * FROM all_types_table;");

  EXPECT_EQ(result.size(), all_types_table->row_count());
  EXPECT_EQ(result[0].size(), static_cast<size_t>(all_types_table->column_count()));

  for (auto row_id = uint64_t{0}; row_id < all_types_table->row_count(); row_id++) {
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
  auto connection = pqxx::connection{_connection_string};

  auto transaction = pqxx::nontransaction{connection};

  transaction.exec("COPY another_table FROM 'resources/test_data/tbl/int_float.tbl';");

  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("another_table"));
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("another_table"), _table_a);
}

TEST_F(ServerTestRunner, TestInvalidCopyImport) {
  auto connection = pqxx::connection{_connection_string};

  auto transaction = pqxx::nontransaction{connection};

  // Ill-formed
  EXPECT_THROW(transaction.exec("COPY another_table FROM;"), pqxx::broken_connection);

  // File is not existing
  EXPECT_THROW(transaction.exec("COPY another_table FROM 'not/existing/file.tbl';"), pqxx::broken_connection);

  // Unsupported file extension
  EXPECT_THROW(transaction.exec("COPY another_table FROM 'resources/test_data/tbl/float';"), pqxx::broken_connection);

  // Check whether server is still running and connection established
  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), _table_a->row_count());
}

TEST_F(ServerTestRunner, TestCopyExport) {
  auto connection = pqxx::connection{_connection_string};

  auto transaction = pqxx::nontransaction{connection};

  transaction.exec("COPY table_a TO '" + _export_filename + ".bin';");

  EXPECT_TRUE(file_exists(_export_filename + ".bin"));
  EXPECT_TRUE(compare_files(_export_filename + ".bin", "resources/test_data/bin/int_float.bin"));
}

TEST_F(ServerTestRunner, TestInvalidCopyExport) {
  auto connection = pqxx::connection{_connection_string};

  auto transaction = pqxx::nontransaction{connection};

  // Ill-formed
  EXPECT_THROW(transaction.exec("COPY table_a TO;"), pqxx::broken_connection);

  // Table is not existing
  EXPECT_THROW(transaction.exec("COPY not_existing TO './does_not_work.tbl';"), pqxx::broken_connection);

  // Unsupported file extension
  EXPECT_THROW(transaction.exec("COPY table_a TO './does_not_work.mp3';"), pqxx::broken_connection);

  // Check whether server is still running and connection established
  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), _table_a->row_count());
}

TEST_F(ServerTestRunner, TestCopyIntegration) {
  auto connection = pqxx::connection{_connection_string};

  auto transaction = pqxx::nontransaction{connection};

  // We delete a tuple of a table and export it.
  transaction.exec("DELETE FROM table_a WHERE a = 123;");
  transaction.exec("COPY table_a TO '" + _export_filename + ".bin';");
  transaction.exec("COPY table_a TO '" + _export_filename + ".csv';");

  EXPECT_TRUE(file_exists(_export_filename + ".bin"));
  EXPECT_TRUE(file_exists(_export_filename + ".csv"));
  EXPECT_TRUE(compare_files(_export_filename + ".bin", "resources/test_data/bin/int_float_deleted.bin"));
  EXPECT_TRUE(compare_files(_export_filename + ".csv", "resources/test_data/bin/int_float_deleted.csv"));

  // Get reference table without deleted row
  auto get_table = std::make_shared<GetTable>("table_a");
  auto validate = std::make_shared<Validate>(get_table);
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);
  validate->set_transaction_context(transaction_context);
  get_table->execute();
  validate->execute();
  const auto expected_table = validate->get_output();

  // Re-import the tables and compare them with the expected one
  transaction.exec("COPY table_b FROM '" + _export_filename + ".bin';");
  transaction.exec("COPY table_c FROM '" + _export_filename + ".csv';");
  const auto table_b = Hyrise::get().storage_manager.get_table("table_b");
  const auto table_c = Hyrise::get().storage_manager.get_table("table_c");

  EXPECT_TABLE_EQ_ORDERED(table_b, expected_table);
  EXPECT_TABLE_EQ_ORDERED(table_c, expected_table);
}

TEST_F(ServerTestRunner, TestInvalidStatement) {
  auto connection = pqxx::connection{_connection_string};

  auto transaction = pqxx::nontransaction{connection};

  // Ill-formed SQL statement
  EXPECT_THROW(transaction.exec("SELECT * FROM;"), pqxx::broken_connection);

  // Well-formed but table does not exist
  EXPECT_THROW(transaction.exec("SELECT * FROM non_existent;"), pqxx::broken_connection);

  // Check whether server is still running and connection established
  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), _table_a->row_count());
}

TEST_F(ServerTestRunner, TestTransactionCommit) {
  auto connection = pqxx::connection{_connection_string};
  pqxx::connection verification_connection{_connection_string};

  pqxx::transaction transaction{connection};
  transaction.exec("INSERT INTO table_a (a, b) VALUES (1, 2);");

  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), 4);

  {
    auto verification_transaction = pqxx::transaction{verification_connection};
    const auto verification_result = verification_transaction.exec("SELECT * FROM table_a;");
    EXPECT_EQ(verification_result.size(), 3);
  }

  transaction.commit();

  {
    auto verification_transaction = pqxx::transaction{verification_connection};
    const auto verification_result = verification_transaction.exec("SELECT * FROM table_a;");
    EXPECT_EQ(verification_result.size(), 4);
  }
}

TEST_F(ServerTestRunner, TestTransactionRollback) {
  auto connection = pqxx::connection{_connection_string};

  pqxx::transaction transaction{connection};
  transaction.exec("INSERT INTO table_a (a, b) VALUES (1, 2);");

  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), 4);

  transaction.abort();

  auto verification_transaction = pqxx::transaction{connection};
  const auto verification_result = verification_transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(verification_result.size(), 3);
}

TEST_F(ServerTestRunner, TestInvalidTransactionFlow) {
  auto connection = pqxx::connection{_connection_string};

  pqxx::transaction transaction{connection};
  EXPECT_THROW(transaction.exec("BEGIN;"), pqxx::broken_connection);
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
  auto connection = pqxx::connection{_connection_string};
  auto transaction = pqxx::nontransaction{connection};

  const auto expected_num_rows = _table_a->row_count() + 1;
  transaction.exec("INSERT INTO table_a VALUES (1, 1.0);");
  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), expected_num_rows);
}

TEST_F(ServerTestRunner, TestShutdownDuringExecution) {
  // Test that open sessions are allowed to finish before the server is destroyed. This is more relevant for tests
  // than for the actual execution. In "real-life", i.e., during our experiments, we usually simply kill the server.
  // In tests however, the server finishing while sessions might not be completely finished could lead to issues
  // like #1977.

  pqxx::connection insert_connection{_connection_string};
  pqxx::nontransaction insert_transaction{insert_connection};
  for (auto i = 0; i < (HYRISE_DEBUG ? 6 : 8); ++i) {
    insert_transaction.exec("INSERT INTO table_a SELECT * FROM table_a;");
  }

  // These should run for a while, one should finish earlier
  std::thread([&] {
    auto connection = pqxx::connection{_connection_string};
    auto transaction = pqxx::nontransaction{connection};
    transaction.exec("SELECT * FROM table_a t1, table_a t2");
  }).detach();

  std::thread([&] {
    auto connection = pqxx::connection{_connection_string};
    auto transaction = pqxx::nontransaction{connection};
    transaction.exec("SELECT * FROM table_a t1, table_a t2 WHERE t1.a = 123");
  }).detach();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // No assertions here. If the server would destroy resources early, we would expect to see errors in asan/tsan and
  // segfaults in regular execution.
}

TEST_F(ServerTestRunner, TestPreparedStatement) {
  auto connection = pqxx::connection{_connection_string};
  auto transaction = pqxx::nontransaction{connection};

  const auto prepared_name = std::string{"statement1"};
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");

  const auto param = 1234u;
  const auto result1 = transaction.exec(pqxx::prepped{prepared_name}, pqxx::params{param});
  EXPECT_EQ(result1.size(), 1u);
  const auto result2 = transaction.exec(pqxx::prepped{prepared_name}, pqxx::params{123});
  EXPECT_EQ(result2.size(), 2u);

  transaction.exec("INSERT INTO table_a VALUES (55555, 1.0);");
  const auto result3 = transaction.exec(pqxx::prepped{prepared_name}, pqxx::params{param});
  EXPECT_EQ(result3.size(), 2u);
}

TEST_F(ServerTestRunner, TestUnnamedPreparedStatement) {
  auto connection = pqxx::connection{_connection_string};
  auto transaction = pqxx::nontransaction{connection};

  const std::string prepared_name = "";
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");

  const auto param = 1234u;
  const auto result1 = transaction.exec(pqxx::prepped{prepared_name}, pqxx::params{param});
  EXPECT_EQ(result1.size(), 1u);

  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a <= ?");

  const auto result2 = transaction.exec(pqxx::prepped{prepared_name}, pqxx::params{param});
  EXPECT_EQ(result2.size(), 2u);
}

TEST_F(ServerTestRunner, TestInvalidPreparedStatement) {
  auto connection = pqxx::connection{_connection_string};
  auto transaction = pqxx::nontransaction{connection};

  const std::string prepared_name = "";
  const auto param = 1234u;

  // Ill-formed prepared statement
  EXPECT_THROW(connection.prepare(prepared_name, "SELECT * FROM WHERE a > ?"), pqxx::broken_connection);

  // Well-formed but table does not exist
  EXPECT_ANY_THROW(connection.prepare(prepared_name, "SELECT * FROM non_existent WHERE a > ?"));

  // Wrong number of parameters
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ? and a > ?");
  EXPECT_ANY_THROW(transaction.exec(pqxx::prepped{prepared_name}, pqxx::params{param}));

  // Check whether server is still running and connection established
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");
  const auto result = transaction.exec(pqxx::prepped{prepared_name}, pqxx::params{param});
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
    auto connection = pqxx::connection{_connection_string};
    auto transaction = pqxx::nontransaction{connection};
    const auto result = transaction.exec(sql);
    EXPECT_EQ(result.size(), expected_num_rows);
  };

  const auto num_threads = size_t{100};
  auto threads = std::vector<std::thread>{};
  threads.reserve(num_threads);

  for (auto thread_num = size_t{0}; thread_num < num_threads; ++thread_num) {
    threads.emplace_back(connection_run);
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(ServerTestRunner, TestTransactionConflicts) {
  // Similar to TestParallelConnections, but this time we modify the table, expecting some conflicts on the way
  // Also similar to StressTest.TestTransactionConflicts, only that we go through the server
  auto initial_sum = int64_t{};
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, table] = pipeline.get_result_table();
    initial_sum = *table->get_value<int64_t>(ColumnID{0}, 0);
  }

  auto successful_increments = std::atomic_int{0};
  auto conflicted_increments = std::atomic_int{0};
  const auto iterations_per_thread = 10;

  // Define the work package
  const auto connection_run = [&]() {
    for (auto iteration = 0; iteration < iterations_per_thread; ++iteration) {
      auto connection = pqxx::connection{_connection_string};
      auto transaction = pqxx::nontransaction{connection};
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

  constexpr auto num_threads = size_t{100};
  auto threads = std::vector<std::thread>{};
  threads.reserve(num_threads);

  for (auto thread_num = size_t{0}; thread_num < num_threads; ++thread_num) {
    threads.emplace_back(connection_run);
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Verify results.
  auto final_sum = int64_t{0};
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, table] = pipeline.get_result_table();
    final_sum = *table->get_value<int64_t>(ColumnID{0}, 0);
  }

  // Really pessimistic, but at least 2 statements should have made it.
  EXPECT_GT(successful_increments, 2);

  // We also want to see at least one conflict so that we can be sure that conflict handling in the server works.
  // In case this fails, start playing with num_threads to provoke more conflicts.
  EXPECT_GE(conflicted_increments, 1);

  EXPECT_EQ(successful_increments + conflicted_increments, num_threads * iterations_per_thread);
  EXPECT_EQ(final_sum - initial_sum, successful_increments);
}

}  // namespace hyrise
