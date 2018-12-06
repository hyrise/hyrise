#pragma once

#include "abstract_query_generator.hpp"
#include "benchmark_utils.hpp"

namespace opossum {

class FileBasedQueryGenerator : public AbstractQueryGenerator {
 public:
  // query_path can be either a folder or a single .sql file
  FileBasedQueryGenerator(const BenchmarkConfig& config, const std::string& query_path);
  std::string build_query(const QueryID query_id) override;

 protected:
  // Get all queries from a given file
  void _parse_query_file(const std::string& query_file);

  std::vector<std::string> _queries;
};

}  // namespace opossum
