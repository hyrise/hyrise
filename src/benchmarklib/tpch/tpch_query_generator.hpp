#pragma once

#include "abstract_query_generator.hpp"

namespace opossum {

class TPCHQueryGenerator : public AbstractQueryGenerator {
 public:
  // We want to provide both "classical" TPC-H queries (i.e., regular SQL queries), and prepared statements. To do so,
  // we use tpch_queries.cpp as a basis and either build PREPARE and EXECUTE statements or replace the question marks
  // with their random values before returning the SQL query.
  TPCHQueryGenerator(bool use_prepared_statements);
  explicit TPCHQueryGenerator(bool use_prepared_statements, const std::vector<QueryID>& selected_queries);

  std::string get_preparation_queries() const override;
  std::string build_query(const QueryID query_id) override;

 protected:
  // Generates the names of the queries (e.g., TPCH1)
  void _generate_names();

  // Generates the PREPARE queries (if needed)
  void _generate_preparation_queries();

  // Builds either an EXECUTE statement or fills the "?" placeholders with values, depending on _use_prepared_statements
  std::string _build_query_with_placeholders(const QueryID query_id, const std::vector<std::string>& parameter_values);

  // Should we use prepared statements or generate "regular" SQL queries?
  const bool _use_prepared_statements;
};

}  // namespace opossum
