#include "abstract_query_generator.hpp"

namespace opossum {

std::string AbstractQueryGenerator::get_preparation_queries() const { return ""; }

size_t AbstractQueryGenerator::selected_query_count() const { return _selected_queries.size(); }

const std::vector<QueryID>& AbstractQueryGenerator::selected_queries() const { return _selected_queries; }

std::string AbstractQueryGenerator::build_deterministic_query(const QueryID query_id) { return build_query(query_id); }

}  // namespace opossum
