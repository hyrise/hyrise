#pragma once

#include "abstract_query_generator.hpp"

namespace opossum {

class TPCHQueryGenerator : public AbstractQueryGenerator {
 public:
  TPCHQueryGenerator();
  explicit TPCHQueryGenerator(const std::vector<QueryID>& selected_queries);
  std::string build_query(const QueryID query_id) override;

 protected:
  void _generate_names();
};

}  // namespace opossum
