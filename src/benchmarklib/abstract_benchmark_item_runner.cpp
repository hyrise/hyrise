#include "abstract_benchmark_item_runner.hpp"

namespace opossum {

std::string AbstractBenchmarkItemRunner::get_preparation_queries() const { return ""; }

size_t AbstractBenchmarkItemRunner::selected_query_count() const { return _selected_queries.size(); }

const std::vector<QueryID>& AbstractBenchmarkItemRunner::selected_queries() const { return _selected_queries; }

std::string AbstractBenchmarkItemRunner::build_deterministic_query(const QueryID query_id) {
  return build_query(query_id);
}

}  // namespace opossum
