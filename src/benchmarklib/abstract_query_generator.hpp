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

  // Gets the SQL string needed to prepare the following queries. Usually, these would be PREPARE statements.
  virtual std::string get_preparation_queries() const;

  // Generates a SQL string for a given (zero-indexed) query id
  virtual std::string build_query(const QueryID query_id) = 0;

  // Some benchmarks might select random parameters (e.g., TPC-H). For verification purposes, this method returns a
  // constant query. The results of this query can then be compared with a known-to-be-good result.
  virtual std::string build_deterministic_query(const QueryID query_id);

  // Returns the names of the individual queries (e.g., "TPC-H 1")
  virtual std::string query_name(const QueryID query_id) const = 0;

  // Returns the number of queries supported by the benchmark
  virtual size_t available_query_count() const = 0;

  // Returns the number of queries selected for execution
  size_t selected_query_count() const;

  // Returns the QueryIDs of all selected queries
  const std::vector<QueryID>& selected_queries() const;

 protected:
  std::vector<QueryID> _selected_queries;

  // PREPARE and other statements that should be executed first
  std::vector<std::string> _preparation_queries;
};

}  // namespace opossum
