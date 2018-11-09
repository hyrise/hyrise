#include "abstract_query_generator.hpp"

namespace opossum {

const std::vector<std::string>& AbstractQueryGenerator::query_names() const { return _query_names; }

QueryID::base_type AbstractQueryGenerator::num_available_queries() const { return _query_names.size(); }

QueryID::base_type AbstractQueryGenerator::num_selected_queries() const { return _selected_queries.size(); }

const std::vector<QueryID>& AbstractQueryGenerator::selected_queries() const { return _selected_queries; }

}  // namespace opossum
