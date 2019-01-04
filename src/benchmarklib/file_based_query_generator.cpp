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
    auto query_file_paths = std::vector<std::filesystem::path>{};

    // Recursively walk through the specified directory and add all files on the way
    for (const auto& entry : filesystem::recursive_directory_iterator(path)) {
      const auto filename = entry.path().string();
      if (filesystem::is_regular_file(entry) && is_sql_file(filename)) {
        query_file_paths.emplace_back(filename);
      }
    }

    // Sort the files alphabetically (so the queries are in a deterministic order)
    std::sort(query_file_paths.begin(), query_file_paths.end(), [&](const auto& lhs, const auto& rhs) {
      return lhs.filename() < rhs.filename();
    });

    // Read the queries from the files
    for (const auto& query_file_path : query_file_paths) {
      _parse_query_file(query_file_path);
    }
  }

  _selected_queries.resize(_query_names.size());
  std::iota(_selected_queries.begin(), _selected_queries.end(), QueryID{0});
}

void FileBasedQueryGenerator::_parse_query_file(const std::string& query_file_path) {
  std::ifstream file(query_file_path);
  const auto filename = filesystem::path{query_file_path}.stem().string();

  std::string content{std::istreambuf_iterator<char>(file), {}};

  /**
   * A file can contain multiple SQL statements, and each statement may cover one or more lines.
   * We use the SQLParser to split up the content of the file into the individual SQL statements.
   */

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parse(content, &parse_result);
  Assert(parse_result.isValid(), create_sql_parser_error_message(content, parse_result));

  std::vector<std::string> queries;

  size_t sql_string_offset{0u};
  for (auto statement_idx = size_t{0}; statement_idx < parse_result.size(); ++statement_idx) {
    const auto query_name = filename + '.' + std::to_string(statement_idx);
    const auto statement_string_length = parse_result.getStatement(statement_idx)->stringLength;
    const auto statement_string = boost::trim_copy(content.substr(sql_string_offset, statement_string_length));
    sql_string_offset += statement_string_length;
    _query_names.emplace_back(query_name);
    _queries.emplace_back(statement_string);
  }

  // More convenient names if there is only one query per file
  if (_queries.size() == 1) {
    auto& query_name = _query_names[0];
    query_name.erase(query_name.end() - 2, query_name.end());  // -2 to remove ".0" at end of name
  }
}

std::string FileBasedQueryGenerator::build_query(const QueryID query_id) { return _queries[query_id]; }

}  // namespace opossum
