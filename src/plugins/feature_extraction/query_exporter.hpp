#pragma once

#include "types.hpp"

namespace opossum {

class QueryExporter {
 public:
  static std::string query_hash(const std::string& query);

  void add_query(const std::string& hash, const std::string& query, const size_t frequency);

  void export_queries(const std::string& file_name);

 protected:
  struct Query {
    Query(const std::string& init_hash, const std::string& init_query, const size_t init_frequency)
        : hash{init_hash}, query{init_query}, frequency{init_frequency} {}
    std::string hash;
    std::string query;
    size_t frequency;
  };

  std::vector<Query> _queries;
};

}  // namespace opossum
