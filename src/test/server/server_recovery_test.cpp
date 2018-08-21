#include <pqxx/pqxx>
#include "utils/filesystem.hpp"

#include "base_test.hpp"

#include "constant_mappings.hpp"
#include "logging/logger.hpp"

namespace opossum {

class ServerRecoveryTest : public BaseTestWithParam<std::pair<Logger::Implementation, Logger::Format>> {
 protected:
  static constexpr char _folder[] = "data/";

  void restart_server(std::pair<Logger::Implementation, Logger::Format> logging) {
    terminate_server();
    start_server(logging);
  }

  void TearDown() override {
    terminate_server();
    if (filesystem::exists(test_data_path + _folder)) {
      filesystem::remove_all(test_data_path + _folder);
    }
  }

  std::tuple<uint32_t, uint32_t> exec(const char* cmd, const std::string& port_str, const std::string& pid_str,
                                      const char delimiter) {
    std::array<char, 128> buffer;
    std::string output;
    std::shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
    if (!pipe) throw std::runtime_error("popen() server failed");
    size_t port_pos;
    size_t pid_pos;
    while (!feof(pipe.get())) {
        if (fgets(buffer.data(), 128, pipe.get()) != nullptr) {
            output += buffer.data();
        }
        port_pos = output.find(port_str);
        pid_pos = output.find(pid_str);
        if (port_pos != std::string::npos && pid_pos != std::string::npos) break;
    }
    auto port = std::stoul(output.substr(port_pos + port_str.length(), output.find(delimiter, port_pos)));
    
    auto pid = std::stoul(output.substr(pid_pos + pid_str.length(), output.find(delimiter, pid_pos)));
    
    return {port, pid};
  }

  void start_server(std::pair<Logger::Implementation, Logger::Format> logging) {
    std::string implementation_string = logger_to_string.left.at(logging.first);
    std::string format_string = log_format_to_string.left.at(logging.second);

    auto cmd =
        "\"" + build_dir + "/hyriseServer\" --logger " + implementation_string + " --log_format " + format_string + " --data_path " + test_data_path + _folder + " &";

    auto [port, server_pid] = exec(cmd.c_str(), "Port: ", "PID: ", '\n');
    _server_pid = server_pid;

    _connection_string = "hostaddr=127.0.0.1 port=" + std::to_string(port);
  }

  void terminate_server() { std::system(("kill " + std::to_string(_server_pid)).c_str()); }

  std::string _connection_string;
  uint32_t _server_pid;
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

std::pair<Logger::Implementation, Logger::Format> loggings[] = {
  {Logger::Implementation::Simple, Logger::Format::Text},
  {Logger::Implementation::Simple, Logger::Format::Binary},
  {Logger::Implementation::GroupCommit, Logger::Format::Text},
  {Logger::Implementation::GroupCommit, Logger::Format::Binary}
};

auto formatter = [](const testing::TestParamInfo<std::pair<Logger::Implementation, Logger::Format>> info) {
  return logger_to_string.left.at(info.param.first) + "_" + log_format_to_string.left.at(info.param.second);
};

INSTANTIATE_TEST_CASE_P(loggings, ServerRecoveryTest, ::testing::ValuesIn(loggings),
                        formatter);

}  // namespace opossum
