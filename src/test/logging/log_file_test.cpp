#include "utils/filesystem.hpp"

#include "base_test.hpp"

#include "constant_mappings.hpp"
#include "logging/logger.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

class LogFileTest : public BaseTestWithParam<std::pair<Logger::Implementation, Logger::Format>> {
 protected:
  static constexpr char _folder[] = "data/";

  void SetUp() override {
    _log_file_path = test_data_path + _folder + Logger::_log_folder + Logger::_filename + "1";

    StorageManager::get().reset();
    SQLQueryCache<SQLQueryPlan>::get().clear();

    auto a_table = load_table("src/test/tables/int_float_double_string.tbl", 2);
    StorageManager::get().add_table("a_table", a_table);
  }

  void TearDown() override {
    Logger::reset_to_no_logger();

    if (filesystem::exists(test_data_path + _folder)) {
      filesystem::remove_all(test_data_path + _folder);
    }
  }

  void run_sql(const std::string& sql) { SQLPipelineBuilder{sql}.create_pipeline().get_result_table(); }

  std::string _log_file_path;
};

/*  
 *  Currently these are not supported, but should be tested when fixed/implemented:
 *    - insert nulls
 *    - delete statements with 'or': "DELETE ... WHERE x=y OR w=z"
 */
TEST_P(LogFileTest, TestWorkflow) {
  Logger::setup(test_data_path + _folder, GetParam().first, GetParam().second);

  run_sql("INSERT INTO a_table VALUES (41, 41.0, 41.0, '41');");
  run_sql("INSERT INTO a_table VALUES (229, 929.7, 14.983, 'öäüia');");
  run_sql("DELETE FROM a_table WHERE i = 2;");
  run_sql("DELETE FROM a_table WHERE s = 'f';");
  run_sql("UPDATE a_table SET i = 7, f = 7.2 WHERE i = 41;");
  run_sql("INSERT INTO a_table VALUES (999, 3.0, 2.0, 'abcde');");
  run_sql("INSERT INTO a_table VALUES (101, 0.123, 3.21, 'xy');");
  run_sql("UPDATE a_table SET i = 471, d = 13.5 WHERE i = 1;");
  run_sql("DELETE FROM a_table WHERE i = 6;");
  run_sql("INSERT INTO a_table VALUES (0, 0.0, 0.0, '');");
  run_sql("DELETE FROM a_table WHERE f = 4.0;");

  std::ifstream result_file(_log_file_path);
  std::string result((std::istreambuf_iterator<char>(result_file)), std::istreambuf_iterator<char>());

  std::ifstream expected_file("src/test/logging/results/" + log_format_to_string.left.at(GetParam().second) +
                              "_log_file_result");
  std::string expected((std::istreambuf_iterator<char>(expected_file)), std::istreambuf_iterator<char>());

  EXPECT_EQ(expected, result);
}

std::pair<Logger::Implementation, Logger::Format> loggings[] = {
    {Logger::Implementation::Simple, Logger::Format::Text},
    {Logger::Implementation::Simple, Logger::Format::Binary},
    {Logger::Implementation::GroupCommit, Logger::Format::Text},
    {Logger::Implementation::GroupCommit, Logger::Format::Binary}};

auto formatter = [](const testing::TestParamInfo<std::pair<Logger::Implementation, Logger::Format>> info) {
  return logger_to_string.left.at(info.param.first) + "_" + log_format_to_string.left.at(info.param.second);
};

INSTANTIATE_TEST_CASE_P(loggings, LogFileTest, ::testing::ValuesIn(loggings), formatter);

}  // namespace opossum
