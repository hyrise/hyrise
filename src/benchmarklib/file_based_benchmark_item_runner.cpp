#include "file_based_benchmark_item_runner.hpp"

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <fstream>

#include "SQLParser.h"
#include "operators/print.hpp"
#include "sql/create_sql_parser_error_message.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

FileBasedBenchmarkItemRunner::FileBasedBenchmarkItemRunner(
    const std::shared_ptr<BenchmarkConfig>& config, const std::string& query_path,
    const std::unordered_set<std::string>& filename_blacklist,
    const std::optional<std::unordered_set<std::string>>& query_subset,
    const std::optional<const std::string>& expected_results_directory_path)
    : AbstractBenchmarkItemRunner(config) {
  const auto is_sql_file = [](const std::string& filename) { return boost::algorithm::ends_with(filename, ".sql"); };

  std::filesystem::path path{query_path};

  Assert(std::filesystem::exists(path), "No such file or directory '" + query_path + "'");

  if (std::filesystem::is_regular_file(path)) {
    Assert(is_sql_file(query_path), "Specified file '" + query_path + "' is not an .sql file");
    _parse_query_file(query_path, query_subset);
  } else {
    // Recursively walk through the specified directory and add all files on the way
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
      if (std::filesystem::is_regular_file(entry) && is_sql_file(entry.path())) {
        if (filename_blacklist.find(entry.path().filename()) != filename_blacklist.end()) {
          continue;
        }
        _parse_query_file(entry.path(), query_subset);
      }
    }
  }

  _items.resize(_queries.size());
  std::iota(_items.begin(), _items.end(), BenchmarkItemID{0});

  // Sort queries by name
  std::sort(_queries.begin(), _queries.end(), [](const Query& lhs, const Query& rhs) { return lhs.name < rhs.name; });

  if (expected_results_directory_path) {
    Assert(std::filesystem::is_directory(*expected_results_directory_path),
           "Expected results path (" + *expected_results_directory_path + ") has to be a directory.");
    _load_expected_results(*expected_results_directory_path);
  }
}

void FileBasedBenchmarkItemRunner::_on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) {
  sql_executor.execute(_queries[item_id].sql);
}

std::string FileBasedBenchmarkItemRunner::item_name(const BenchmarkItemID item_id) const {
  return _queries[item_id].name;
}

const std::vector<BenchmarkItemID>& FileBasedBenchmarkItemRunner::items() const { return _items; }

void FileBasedBenchmarkItemRunner::_parse_query_file(
    const std::filesystem::path& query_file_path, const std::optional<std::unordered_set<std::string>>& query_subset) {
  std::ifstream file(query_file_path);

  // The names of queries from, e.g., "queries/TPCH-7.sql" will be prefixed with "TPCH-7."
  const auto item_name_prefix = query_file_path.stem().string();

  std::string content{std::istreambuf_iterator<char>(file), {}};

  /**
   * A file can contain multiple SQL statements, and each statement may cover one or more lines.
   * We use the SQLParser to split up the content of the file into the individual SQL statements.
   */

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parse(content, &parse_result);
  Assert(parse_result.isValid(), create_sql_parser_error_message(content, parse_result));

  std::vector<Query> queries_in_file{parse_result.size()};

  size_t sql_string_offset{0u};
  for (auto statement_idx = size_t{0}; statement_idx < parse_result.size(); ++statement_idx) {
    const auto item_name = item_name_prefix + '.' + std::to_string(statement_idx);
    const auto statement_string_length = parse_result.getStatement(statement_idx)->stringLength;
    const auto statement_string = boost::trim_copy(content.substr(sql_string_offset, statement_string_length));
    sql_string_offset += statement_string_length;
    queries_in_file[statement_idx] = {item_name, statement_string};
  }

  // Remove ".0" from the end of the query name if there is only one file
  if (queries_in_file.size() == 1) {
    queries_in_file.front().name.erase(queries_in_file.front().name.end() - 2, queries_in_file.front().name.end());
  }

  /**
   * Add queries to _queries and _item_names, if query_subset allows it
   */
  for (const auto& query : queries_in_file) {
    if (!query_subset || query_subset->count(query.name)) {
      _queries.emplace_back(query);
    }
  }
}

void FileBasedBenchmarkItemRunner::_load_expected_results(const std::filesystem::path& results_directory_path) {
  const auto is_tbl_file = [](const std::string& filename) { return boost::algorithm::ends_with(filename, ".tbl"); };
  _expected_results.resize(_queries.size());

  std::cout << "- Loading expected result tables" << "\n";

  for (const auto& entry : std::filesystem::recursive_directory_iterator(results_directory_path)) {
    if (std::filesystem::is_regular_file(entry) && is_tbl_file(entry.path())) {
      const auto query_name = entry.path().stem().string();
      const auto iter = std::find_if(_queries.cbegin(), _queries.cend(),
                                     [&query_name](const auto& query) { return query.name == query_name; });
      if (iter != _queries.cend()) {
        const auto item_id = std::distance(_queries.cbegin(), iter);
        std::cout << "-  Loading result table " + entry.path().string() << "\n";
        _expected_results[item_id] = load_table(entry.path().string());
      }
    }
  }
}

}  // namespace opossum
