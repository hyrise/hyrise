#include "abstract_query_generator.hpp"

namespace opossum {

const std::vector<std::string>& AbstractQueryGenerator::query_names() const { return _query_names; }

size_t AbstractQueryGenerator::available_query_count() const { return _query_names.size(); }

size_t AbstractQueryGenerator::selected_query_count() const { return _selected_queries.size(); }

const std::vector<QueryID>& AbstractQueryGenerator::selected_queries() const { return _selected_queries; }

}  // namespace opossum
