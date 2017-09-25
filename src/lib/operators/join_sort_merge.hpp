#pragma once

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "product.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "types.hpp"

namespace opossum {

  /**
   * This operator joins two tables using one column of each table by performing radix-partition-sort and a merge join.
   * The output is a new table with referenced columns for all columns of the two inputs and filtered pos_lists.
   *
   * As with most operators, we do not guarantee a stable operation with regards to positions -
   * i.e., your sorting order might be disturbed.
   *
   * Note: SortMergeJoin does not support null values in the input at the moment.
   * Note: Cross joins are not supported. Use the product operator instead.
   * Note: Outer joins are only implemented for the equi-join case, i.e. the "=" operator.
   */
class JoinSortMerge : public AbstractJoinOperator {
 public:
  JoinSortMerge(const std::shared_ptr<const AbstractOperator> left, const std::shared_ptr<const AbstractOperator> right,
                const JoinMode mode, const std::pair<ColumnID, ColumnID>& column_ids, const ScanType op);

  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override;
  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  template <typename T>
  class JoinSortMergeImpl;

  std::unique_ptr<AbstractJoinOperatorImpl> _impl;
};

}  // namespace opossum
