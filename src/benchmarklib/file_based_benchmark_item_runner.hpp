#pragma once

#include <unordered_set>

#include "abstract_benchmark_item_runner.hpp"
#include "benchmark_config.hpp"

namespace opossum {

class FileBasedBenchmarkItemRunner : public AbstractBenchmarkItemRunner {
 public:
  // @param query_path            can be either a folder or a single .sql file
  // @param filename_blacklist    list of filenames to ignore
  // @param query_subset          if set, only the queries with the specified names (derived from the filename) will be
  //                              generated. If "q7.sql" contains a single query, the query has the name "q7". If
  //                              it contains multiple queries, they are called "q7.0", "q7.1", ...
  FileBasedBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config, const std::string& query_path,
                               const std::unordered_set<std::string>& filename_blacklist = {},
                               const std::optional<std::unordered_set<std::string>>& query_subset = {});

  std::string item_name(const BenchmarkItemID item_id) const override;
  const std::vector<BenchmarkItemID>& items() const override;

 protected:
  bool _on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) override;

  struct Query {
    std::string name;
    std::string sql;
  };

  // Get all queries from a given file
  void _parse_query_file(const std::filesystem::path& query_file_path,
                         const std::optional<std::unordered_set<std::string>>& query_subset);

  std::vector<Query> _queries;
  std::vector<BenchmarkItemID> _items;
};

}  // namespace opossum
