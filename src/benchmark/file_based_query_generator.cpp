#include "file_based_query_generator.hpp"

#include <boost/algorithm/string.hpp>
#include <fstream>

#include "SQLParser.h"
#include "sql/create_sql_parser_error_message.hpp"
#include "utils/assert.hpp"
#include "utils/filesystem.hpp"

namespace opossum {

FileBasedQueryGenerator::FileBasedQueryGenerator(const BenchmarkConfig& config, const std::string& query_path) {
  const auto is_sql_file = [](const std::string& filename) { return boost::algorithm::ends_with(filename, ".sql"); };

  filesystem::path path{query_path};
  Assert(filesystem::exists(path), "No such file or directory '" + query_path + "'");

  if (filesystem::is_regular_file(path)) {
    Assert(is_sql_file(query_path), "Specified file '" + query_path + "' is not an .sql file");
    _parse_query_file(query_path);
  } else {
    // Recursively walk through the specified directory and add all files on the way
    for (const auto& entry : filesystem::recursive_directory_iterator(path)) {
      const auto filename = entry.path().string();
      if (filesystem::is_regular_file(entry) && is_sql_file(filename)) {
        _parse_query_file(filename);
      }
    }
  }

  _selected_queries.resize(_query_names.size());
  std::iota(_selected_queries.begin(), _selected_queries.end(), QueryID{0});
}

void FileBasedQueryGenerator::_parse_query_file(const std::string& query_path) {
  std::ifstream file(query_path);
  const auto filename = filesystem::path{query_path}.stem().string();

  std::string content{std::istreambuf_iterator<char>(file), {}};

  /**
   * A file can contain multiple SQL statements, and each statement may cover one or more lines.
   * We use the SQLParser to split up the content of the file into the individual SQL statements.
   */

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parse(content, &parse_result);
  Assert(parse_result.isValid(), create_sql_parser_error_message(content, parse_result));

  size_t sql_string_offset{0u};
  for (auto statement_idx = int{0}; statement_idx < parse_result.size(); ++statement_idx) {
    const auto query_name = filename + '.' + std::to_string(statement_idx);
    const auto statement_string_length = parse_result.getStatement(statement_idx)->stringLength;
    const auto statement_string = boost::trim_copy(content.substr(sql_string_offset, statement_string_length));
    sql_string_offset += statement_string_length;
    _query_names.emplace_back(query_name);
    _queries.emplace_back(std::move(statement_string));
  }

  // More convenient names if there is only one query per file
  if (_queries.size() == 1) {
    auto& query_name = _query_names[0];
    query_name.erase(query_name.end() - 2, query_name.end());  // -2 because .0 at end of name
  }
}

std::string FileBasedQueryGenerator::build_query(const QueryID query_id) { return _queries[query_id]; }

}  // namespace opossum
