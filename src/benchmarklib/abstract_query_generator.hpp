#pragma once

#include <string>
#include <vector>

#include "strong_typedef.hpp"

STRONG_TYPEDEF(size_t, QueryID);

namespace opossum {

class AbstractQueryGenerator {
 public:
  virtual ~AbstractQueryGenerator() = default;

  const std::vector<std::string>& query_names() const;

  virtual std::string build_query(const QueryID query_id) = 0;

  QueryID::base_type num_available_queries() const;
  QueryID::base_type num_selected_queries() const;
  const std::vector<QueryID>& selected_queries() const;

 protected:
  std::vector<QueryID> _selected_queries;

  // Contains ALL query names, not only the selected ones
  std::vector<std::string> _query_names;
};

}  // namespace opossum
