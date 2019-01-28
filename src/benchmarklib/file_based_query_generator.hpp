#pragma once

#include <unordered_set> // NEEDEDINCLUDE

#include "abstract_query_generator.hpp" // NEEDEDINCLUDE
#include "benchmark_config.hpp" // NEEDEDINCLUDE

namespace opossum {

class FileBasedQueryGenerator : public AbstractQueryGenerator {
 public:
  // @param query_path            can be either a folder or a single .sql file
  // @param filename_blacklist    list of filenames to ignore
  // @param query_subset          if set, only the queries with the specified names (derived from the filename) will be
  //                              generated. If "q7.sql" contains a single query, the query has the name "q7". If
  //                              it contains multiple queries, they are called "q7.0", "q7.1", ...
  FileBasedQueryGenerator(const BenchmarkConfig& config, const std::string& query_path,
                          const std::unordered_set<std::string>& filename_blacklist = {},
                          const std::optional<std::unordered_set<std::string>>& query_subset = {});
  std::string build_query(const QueryID query_id) override;
  std::string query_name(const QueryID query_id) const override;
  size_t available_query_count() const override;

 protected:
  struct Query {
    std::string name;
    std::string sql;
  };

  // Get all queries from a given file
  void _parse_query_file(const std::string& query_file_path,
                         const std::optional<std::unordered_set<std::string>>& query_subset);

  std::vector<Query> _queries;
};

}  // namespace opossum
