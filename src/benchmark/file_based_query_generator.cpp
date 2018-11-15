#include "file_based_query_generator.hpp"

#include <boost/algorithm/string.hpp>
#include <fstream>

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

void FileBasedQueryGenerator::_parse_query_file(const std::string& query_file) {
  auto query_id = 0u;

  std::ifstream file(query_file);
  const auto filename = filesystem::path{query_file}.stem().string();

  std::string query;
  while (std::getline(file, query)) {
    if (query.empty() || query.substr(0, 2) == "--") {
      continue;
    }

    const auto query_name = filename + '.' + std::to_string(query_id);
    _query_names.emplace_back(query_name);
    _queries.emplace_back(std::move(query));
    query_id++;
  }

  // More convenient names if there is only one query per file
  if (query_id == 1) {
    auto& query_name = _query_names.back();
    query_name.erase(query_name.end() - 2, query_name.end());  // -2 because .0 at end of name
  }
}

std::string FileBasedQueryGenerator::build_query(const QueryID query_id) { return _queries[query_id]; }

}  // namespace opossum
