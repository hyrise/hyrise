#include "create_sql_parser_error_message.hpp"

#include <sstream>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"

namespace opossum {

std::string create_sql_parser_error_message(const std::string& sql, const hsql::SQLParserResult& result) {
  std::stringstream error_msg;
  error_msg << "SQL query not valid.\n";

  std::vector<std::string> sql_lines;
  boost::algorithm::split(sql_lines, sql, boost::is_any_of("\n"));

  error_msg << "SQL query:\n==========\n";
  const uint32_t error_line = result.errorLine();
  for (auto line_number = size_t{0}; line_number < sql_lines.size(); ++line_number) {
    error_msg << sql_lines[line_number] << '\n';

    // Add indicator to where the error is
    if (line_number == error_line) {
      const uint32_t error_column = result.errorColumn();
      const auto& line = sql_lines[line_number];

      // Keep indentation of tab characters
      auto num_tabs = std::count(line.begin(), line.begin() + error_column, '\t');
      error_msg << std::string(num_tabs, '\t');

      // Use some color to highlight the error
      const auto color_red = "\x1B[31m";
      const auto color_reset = "\x1B[0m";
      error_msg << std::string(error_column - num_tabs, ' ') << color_red << "^=== ERROR HERE!" << color_reset << "\n";
    }
  }

  error_msg << "=========="
            << "\nError line: " << result.errorLine() << "\nError column: " << result.errorColumn()
            << "\nError message: " << result.errorMsg();

  return error_msg.str();
}

}  // namespace opossum
