#if __has_include(<filesystem>)
#include <filesystem>
namespace filesystem = std::filesystem;
#else
#include <experimental/filesystem>
namespace filesystem = std::experimental::filesystem;
#endif

#include <pqxx/pqxx>

#include <thread>
#include <exception>

#include "base_test.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"

#include "server/server.hpp"
#include "concurrency/logging/logger.hpp"

namespace opossum {

std::string str(Logger::Implementation implementation) {
  switch (implementation) {
    case Logger::Implementation::Simple: return "SimpleLogger";
    case Logger::Implementation::GroupCommit: return "GroupCommitLogger";
    default: return "unknown";
  }
}

class ServerRecoveryTest : public BaseTestWithParam<Logger::Implementation> {
 protected:
  
  static constexpr char _folder[6] = "data/";

  void restart_server(Logger::Implementation implementation) {
    terminate_server();
    start_server(implementation);
  }

  void TearDown() override {
    terminate_server();
    if (filesystem::exists(test_data_path + _folder)) {
      filesystem::remove_all(test_data_path + _folder);
    }
  }

  void start_server(Logger::Implementation implementation) {
    std::string implementation_string = str(implementation);

    // TODO: auto port
    auto cmd = "\"" + build_dir + "/hyriseServer\" 1234 " + implementation_string + " " + test_data_path + _folder + " &";
    std::system(cmd.c_str());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    _connection_string = "hostaddr=127.0.0.1 port=1234";
  }

  void terminate_server() {
    std::system("pkill hyriseServer");
  }

  std::string _connection_string;
};

// // currently doubles won't be logged correctly
// // currently it is not possible to insert nulls
// TEST_P(ServerRecoveryTest, TestWorkflow) {
//   start_server(GetParam());

//   pqxx::connection connection{_connection_string};
//   pqxx::nontransaction transaction{connection};

//   EXPECT_THROW(transaction.exec("SELECT * FROM a_table;"), std::exception);

//   transaction.exec("load src/test/tables/int_float_double_string_null.tbl a_table;");
//   const auto result = transaction.exec("SELECT * FROM a_table;");
//   EXPECT_EQ(result.size(), 6u);

//   transaction.exec("INSERT INTO a_table VALUES (41, 41.0, 41.0, '41');");
//   transaction.exec("INSERT INTO a_table VALUES (229, 929.7, 14.983, 'öäüia');");
//   transaction.exec("DELETE FROM a_table WHERE i = 2 or s = 'f';");
//   transaction.exec("UPDATE a_table SET i = 7, f = 7.2 WHERE i = 41;");
//   transaction.exec("INSERT INTO a_table VALUES (999, null, null, 'abcde');");
//   transaction.exec("INSERT INTO a_table VALUES (null, 0.123, 3.21, 'xy');");
//   transaction.exec("UPDATE a_table SET i = null, d = null WHERE i = 1;");
//   transaction.exec("DELETE FROM a_table WHERE i = 6;");
//   transaction.exec("INSERT INTO a_table VALUES (null, null, null, null);");
//   transaction.exec("DELETE FROM a_table WHERE f = 4.0;");

//   restart_server(GetParam());

//   pqxx::connection connection2{_connection_string};
//   pqxx::nontransaction transaction2{connection2};
//   const auto result2 = transaction2.exec("SELECT * FROM a_table;");

//   EXPECT_EQ(result2.size(), 7u);

//   int i;
//   float f;
//   double d;
//   std::string s;

//   // TODO
//   result2[0][0].to(i);
//   result2[0][1].to(f);
//   result2[0][2].to(d);
//   result2[0][3].to(s);
//   EXPECT_EQ(i, );
//   EXPECT_EQ(f, );
//   EXPECT_EQ(d, );
//   EXPECT_EQ(s, );
// }

TEST_P(ServerRecoveryTest, TestWorkflowWithIntAndString) {
  start_server(GetParam());

  std::this_thread::sleep_for(std::chrono::milliseconds(3000));

  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  EXPECT_THROW(transaction.exec("SELECT * FROM a_table;"), std::exception);

  transaction.exec("load src/test/tables/int_string.tbl a_table;");

  const auto result = transaction.exec("SELECT * FROM a_table;");
  EXPECT_EQ(result.size(), 12u);

  transaction.exec("INSERT INTO a_table VALUES (41, '41');");
  transaction.exec("INSERT INTO a_table VALUES (229, 'öäüia');");
  transaction.exec("DELETE FROM a_table WHERE a = 4 or b = 'test16';");
  transaction.exec("UPDATE a_table SET a = 9001, b = 'some string' WHERE a = 41;");
  transaction.exec("INSERT INTO a_table VALUES (999, 'abcde');");
  transaction.exec("INSERT INTO a_table VALUES (131, 'xy');");
  transaction.exec("UPDATE a_table SET a= 12, b = '' WHERE a = 2;");
  transaction.exec("DELETE FROM a_table WHERE a = 6;");
  transaction.exec("INSERT INTO a_table VALUES (0, 'test0');");
  transaction.exec("DELETE FROM a_table WHERE a = 12 or b = 'test20' or b = 'test19';");
  transaction.exec("DELETE FROM a_table WHERE b = 'test18' or a = 10;");

  restart_server(GetParam());

  pqxx::connection connection2{_connection_string};
  pqxx::nontransaction transaction2{connection2};
  const auto result2 = transaction2.exec("SELECT * FROM a_table;");

  EXPECT_EQ(result2.size(), 8u);

  int i;
  std::string s;

  result2[0][0].to(i);
  result2[0][1].to(s);
  EXPECT_EQ(i, 8);
  EXPECT_EQ(s, "test8");
  result2[1][0].to(i);
  result2[1][1].to(s);
  EXPECT_EQ(i, 14);
  EXPECT_EQ(s, "test14");
  result2[2][0].to(i);
  result2[2][1].to(s);
  EXPECT_EQ(i, 21);
  EXPECT_EQ(s, "test21");
  result2[3][0].to(i);
  result2[3][1].to(s);
  EXPECT_EQ(i, 229);
  EXPECT_EQ(s, "öäüia");
  result2[4][0].to(i);
  result2[4][1].to(s);
  EXPECT_EQ(i, 9001);
  EXPECT_EQ(s, "some string");
  result2[5][0].to(i);
  result2[5][1].to(s);
  EXPECT_EQ(i, 999);
  EXPECT_EQ(s, "abcde");
  result2[6][0].to(i);
  result2[6][1].to(s);
  EXPECT_EQ(i, 131);
  EXPECT_EQ(s, "xy");
  result2[7][0].to(i);
  result2[7][1].to(s);
  EXPECT_EQ(i, 0);
  EXPECT_EQ(s, "test0");
}

Logger::Implementation logging_implementations[] = {
  Logger::Implementation::Simple, 
  Logger::Implementation::GroupCommit
};

auto formatter = [](const testing::TestParamInfo<Logger::Implementation> info) {
  return str(info.param);
};

INSTANTIATE_TEST_CASE_P(logging_implementations, ServerRecoveryTest, ::testing::ValuesIn(logging_implementations), formatter);

}  // namespace opossum
