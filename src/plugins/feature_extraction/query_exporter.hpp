#pragma once

#include "feature_types.hpp"

namespace opossum {

class QueryExporter {
 public:
  static std::string query_hash(const std::string& query);

  void add_query(const std::string& hash, const std::string& query, const size_t frequency);

  void export_queries(const std::string& file_name);

 protected:
  std::vector<Query> _queries;
};

}  // namespace opossum
