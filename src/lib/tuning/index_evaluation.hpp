#pragma once

#include <string>

#include "storage/table.hpp"
#include "storage/index/column_index_type.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

/**
 * An IndexEvaluation contains the characteristics of one particular index
 * as recognized by an AbstractIndexEvaluator
 */
class IndexEvaluation {
 public:
  IndexEvaluation(const std::string& table_name, ColumnID column_id, bool exists = false)
      : table_name{table_name},
        column_id{column_id},
        desirablility{0.0f},
        exists{exists},
        type{ColumnIndexType::Invalid},
        memory_cost{0.0f} {}

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
   * Does this Evaluation refer to an already created index or one that does not exist yet
   */
  bool exists;

  /**
   * exists == true: The type of the existing index
   * exists == false: A proposal for an appropriate index type
   */
  ColumnIndexType type;

  /**
   * exists == true: Memory cost in MiB of the index as reported by the index implementation
   * exists == false: Memory cost in MiB as predicted by the index implementation
   *                    assuming an equal value distribution across chunks
   * Measured in MiB
   */
  float memory_cost;

  /**
   * Operators to allow comparison based on desirability
   */
  bool operator<(const IndexEvaluation& other) const { return (desirablility < other.desirablility); }
  bool operator>(const IndexEvaluation& other) const { return (desirablility > other.desirablility); }

  /**
   * Operator for printing them (debugging)
   */
  friend std::ostream& operator<<(std::ostream& output, IndexEvaluation& evaluation) {
    auto table_ptr = StorageManager::get().get_table(evaluation.table_name);
    auto& column_name = table_ptr->column_name(evaluation.column_id);

    return output << "IndexEvaluation for " << evaluation.table_name << "." << column_name << ": "
                  << evaluation.desirablility << "% (memory cost: " << evaluation.memory_cost
                  << " MiB, exists: " << evaluation.exists << ")";
  }
};

}  // namespace opossum
