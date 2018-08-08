#include <pqxx/pqxx>
#include "utils/filesystem.hpp"

#include "base_test.hpp"

#include "concurrency/logging/logger.hpp"

namespace opossum {

std::string str(Logger::Implementation implementation) {
  switch (implementation) {
    case Logger::Implementation::Simple:
      return "SimpleLogger";
    case Logger::Implementation::GroupCommit:
      return "GroupCommitLogger";
    default:
      return "unknown";
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

    auto cmd =
        "\"" + build_dir + "/hyriseServer\" 1234 " + implementation_string + " " + test_data_path + _folder + " &";
    std::system(cmd.c_str());

    _connection_string = "hostaddr=127.0.0.1 port=1234";

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  void terminate_server() { std::system("pkill hyriseServer"); }

  std::string _connection_string;
};

/*  
 *  Currently these are not supported, but should be tested when fixed/implemented:
 *    - insert nulls
 *    - delete statements with 'or': "DELETE ... WHERE x=y OR w=z"
 */
TEST_P(ServerRecoveryTest, TestWorkflow) {
  start_server(GetParam());

  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  EXPECT_THROW(transaction.exec("SELECT * FROM a_table;"), std::exception);

  transaction.exec("load src/test/tables/int_float_double_string.tbl a_table;");
  const auto result = transaction.exec("SELECT * FROM a_table;");
  EXPECT_EQ(result.size(), 6u);

  transaction.exec("INSERT INTO a_table VALUES (41, 41.0, 41.0, '41');");
  transaction.exec("INSERT INTO a_table VALUES (229, 929.7, 14.983, 'öäüia');");
  transaction.exec("DELETE FROM a_table WHERE i = 2;");
  transaction.exec("DELETE FROM a_table WHERE s = 'f';");
  transaction.exec("UPDATE a_table SET i = 7, f = 7.2 WHERE i = 41;");
  transaction.exec("INSERT INTO a_table VALUES (999, 3.0, 2.0, 'abcde');");
  transaction.exec("INSERT INTO a_table VALUES (101, 0.123, 3.21, 'xy');");
  transaction.exec("UPDATE a_table SET i = 471, d = 13.5 WHERE i = 1;");
  transaction.exec("DELETE FROM a_table WHERE i = 6;");
  transaction.exec("INSERT INTO a_table VALUES (0, 0.0, 0.0, '');");
  transaction.exec("DELETE FROM a_table WHERE f = 4.0;");

  restart_server(GetParam());

  pqxx::connection connection2{_connection_string};
  pqxx::nontransaction transaction2{connection2};
  const auto result2 = transaction2.exec("SELECT * FROM a_table;");

  EXPECT_EQ(result2.size(), 7u);

  int i;
  float f;
  double d;
  std::string s;

  result2[0][0].to(i);
  result2[0][1].to(f);
  result2[0][2].to(d);
  result2[0][3].to(s);
  EXPECT_EQ(i, 3);
  EXPECT_EQ(f, 3.0f);
  EXPECT_EQ(d, 3.0);
  EXPECT_EQ(s, "d");
  result2[1][0].to(i);
  result2[1][1].to(f);
  result2[1][2].to(d);
  result2[1][3].to(s);
  EXPECT_EQ(i, 229);
  EXPECT_EQ(f, 929.7f);
  EXPECT_EQ(d, 14.983);
  EXPECT_EQ(s, "öäüia");
  result2[2][0].to(i);
  result2[2][1].to(f);
  result2[2][2].to(d);
  result2[2][3].to(s);
  EXPECT_EQ(i, 7);
  EXPECT_EQ(f, 7.2f);
  EXPECT_EQ(d, 41.0);
  EXPECT_EQ(s, "41");
  result2[3][0].to(i);
  result2[3][1].to(f);
  result2[3][2].to(d);
  result2[3][3].to(s);
  EXPECT_EQ(i, 999);
  EXPECT_EQ(f, 3.0f);
  EXPECT_EQ(d, 2.0);
  EXPECT_EQ(s, "abcde");
  result2[4][0].to(i);
  result2[4][1].to(f);
  result2[4][2].to(d);
  result2[4][3].to(s);
  EXPECT_EQ(i, 101);
  EXPECT_EQ(f, 0.123f);
  EXPECT_EQ(d, 3.21);
  EXPECT_EQ(s, "xy");
  result2[5][0].to(i);
  result2[5][1].to(f);
  result2[5][2].to(d);
  result2[5][3].to(s);
  EXPECT_EQ(i, 471);
  EXPECT_EQ(f, 1.0);
  EXPECT_EQ(d, 13.5);
  EXPECT_EQ(s, "b");
  result2[6][0].to(i);
  result2[6][1].to(f);
  result2[6][2].to(d);
  result2[6][3].to(s);
  EXPECT_EQ(i, 0);
  EXPECT_EQ(f, 0.0f);
  EXPECT_EQ(d, 0.0);
  EXPECT_EQ(s, "");
}

Logger::Implementation logging_implementations[] = {Logger::Implementation::Simple,
                                                    Logger::Implementation::GroupCommit};

auto formatter = [](const testing::TestParamInfo<Logger::Implementation> info) { return str(info.param); };

INSTANTIATE_TEST_CASE_P(logging_implementations, ServerRecoveryTest, ::testing::ValuesIn(logging_implementations),
                        formatter);

}  // namespace opossum
