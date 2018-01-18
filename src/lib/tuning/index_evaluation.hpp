#pragma once

#include <string>

#include "types.hpp"

namespace opossum {

/**
 * An IndexEvaluation contains the characteristics of one particular index
 * as recognized by an AbstractIndexEvaluator
 */
class IndexEvaluation {
 public:
  IndexEvaluation(const std::string& table_name, ColumnID column_id, bool exists = false)
      : table_name{table_name}, column_id{column_id}, desirablility{0.0f}, memory_cost{0.0f}, exists{exists} {}

  /**
   * The column the this index refers to
   */
  std::string table_name;
  ColumnID column_id;

  /**
   * An IndexEvaluator specific, signed value that indicates
   * how this index will affect the overall system performance
   *
   * desirability values are relative and only comparable if estimated
   * by the same IndexEvaluator
   */
  float desirablility;

  /**
   * An estimate of the index memory footprint in MiB
   */
  float memory_cost;

  /**
   * Does this Evaluation refer to an already created index or one that does not exist yet
   */
  bool exists;

  /**
   * Operators to allow comparison based on desirability
   */
  bool operator<(const IndexEvaluation& other) const { return (desirablility < other.desirablility); }
  bool operator>(const IndexEvaluation& other) const { return (desirablility > other.desirablility); }
};

}  // namespace opossum
