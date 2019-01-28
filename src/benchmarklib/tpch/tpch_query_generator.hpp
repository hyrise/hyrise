#pragma once

#include <atomic>

#include "abstract_query_generator.hpp"

namespace opossum {

class TPCHQueryGenerator : public AbstractQueryGenerator {
 public:
  // We want to provide both "classical" TPC-H queries (i.e., regular SQL queries), and prepared statements. To do so,
  // we use tpch_queries.cpp as a basis and either build PREPARE and EXECUTE statements or replace the question marks
  // with their random values before returning the SQL query.
  TPCHQueryGenerator(bool use_prepared_statements, float scale_factor);
  TPCHQueryGenerator(bool use_prepared_statements, float scale_factor, const std::vector<QueryID>& selected_queries);

  std::string get_preparation_queries() const override;
  std::string build_query(const QueryID query_id) override;
  std::string build_deterministic_query(const QueryID query_id) override;
  std::string query_name(const QueryID query_id) const override;
  size_t available_query_count() const override;

 protected:
  // Generates the PREPARE queries (if needed)
  void _generate_preparation_queries();

  // Builds either an EXECUTE statement or fills the "?" placeholders with values, depending on _use_prepared_statements
  std::string _build_executable_query(const QueryID query_id, const std::vector<std::string>& parameter_values);

  // Should we use prepared statements or generate "regular" SQL queries?
  const bool _use_prepared_statements;

  const float _scale_factor;

  // Used for naming the views generated in query 15
  size_t _q15_view_id = 0;

  // We want deterministic seeds, but since the engine is thread-local, we need to make sure that each thread has its
  // own seed.
  std::atomic<unsigned int> _random_seed{0};
};

}  // namespace opossum
