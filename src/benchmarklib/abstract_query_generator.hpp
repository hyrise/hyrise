#pragma once

#include <string>
#include <vector>

#include "strong_typedef.hpp"

STRONG_TYPEDEF(size_t, QueryID);

namespace opossum {

// Query generators are used by benchmarks to generate SQL strings for the different benchmark queries. In their
// simplest form, they return the same query each time `build_query` is invoked. For benchmarks like TPC-H, they can
// also randomize parameters.

class AbstractQueryGenerator {
 public:
  virtual ~AbstractQueryGenerator() = default;

  // Generates a SQL string for a given (zero-indexed) query id
  virtual std::string build_query(const QueryID query_id) = 0;

  // Returns the names of the individual queries (e.g., "TPC-H 1")
  const std::vector<std::string>& query_names() const;

  // Returns the number of queries supported by the benchmark
  size_t available_query_count() const;

  // Returns the number of queries selected for execution
  size_t selected_query_count() const;

  // Returns the QueryIDs of all selected queries
  const std::vector<QueryID>& selected_queries() const;

 protected:
  std::vector<QueryID> _selected_queries;

  // Contains ALL query names, not only the selected ones
  std::vector<std::string> _query_names;
};

}  // namespace opossum
