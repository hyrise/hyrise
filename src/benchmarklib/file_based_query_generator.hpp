#pragma once

#include <unordered_set>

#include "abstract_query_generator.hpp"
#include "benchmark_config.hpp"

namespace opossum {

class FileBasedQueryGenerator : public AbstractQueryGenerator {
 public:
  // query_path can be either a folder or a single .sql file
  // @param filename_blacklist    list of filenames to ignore
  FileBasedQueryGenerator(const BenchmarkConfig& config, const std::string& query_path,
                          const std::unordered_set<std::string>& filename_blacklist = {});
  std::string build_query(const QueryID query_id) override;

 protected:
  // Get all queries from a given file
  void _parse_query_file(const std::filesystem::path& query_file_path);

  std::vector<std::string> _queries;
};

}  // namespace opossum
