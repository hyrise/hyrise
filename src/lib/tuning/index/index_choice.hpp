#pragma once

#include <set>

#include "storage/index/column_index_type.hpp"
#include "tuning/index/column_ref.hpp"
#include "tuning/tuning_choice.hpp"

namespace opossum {

/**
 * An IndexChoice contains the characteristics of one particular index
 * as recognized by an AbstractIndexEvaluator
 */
class IndexChoice : public TuningChoice {
 public:
  explicit IndexChoice(ColumnRef column_ref, bool exists = false)
      : column_ref{column_ref}, saved_work{0.0f}, exists{exists}, type{ColumnIndexType::Invalid}, memory_cost{0.0f} {}

  float desirability() const final;

  float cost() const final;

  float confidence() const final;

  bool is_currently_chosen() const final;

  void print_on(std::ostream& output) const final;

  /**
    * Notice: we decided to keep public member variables and not create explicit setters/getters
    * beyond the TuningChoice interface, since this class is 95% data object and 5% virtual.
    */

  /**
   * The column the this index refers to
   */
  ColumnRef column_ref;

  /**
   * An IndexEvaluator specific, signed value that indicates
   * how this index will affect the overall system performance
   *
   * desirability values are relative and only comparable if estimated
   * by the same IndexEvaluator
   */
  float saved_work;

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

 protected:
  std::shared_ptr<TuningOperation> _accept_operation() const final;
  std::shared_ptr<TuningOperation> _reject_operation() const final;
};

}  // namespace opossum
