#include "tpch_query_generator.hpp"

#include <numeric>

#include "tpch_queries.hpp"
#include "utils/assert.hpp"

namespace opossum {

TPCHQueryGenerator::TPCHQueryGenerator() {
  _generate_names();
  _selected_queries.resize(22);
  std::iota(_selected_queries.begin(), _selected_queries.end(), QueryID{0});
}

TPCHQueryGenerator::TPCHQueryGenerator(const std::vector<QueryID>& selected_queries) {
  _generate_names();
  _selected_queries = selected_queries;
}

void TPCHQueryGenerator::_generate_names() {
  _query_names.reserve(22);
  for (auto i = 0; i < 22; ++i) {
    _query_names.emplace_back(std::string("TPC-H ") + std::to_string(i + 1));
  }
}

std::string TPCHQueryGenerator::build_query(const QueryID query_id) {
  DebugAssert(query_id < 22, "There are only 22 TPC-H queries");
  return tpch_queries.find(query_id + 1)->second;
}

}  // namespace opossum
