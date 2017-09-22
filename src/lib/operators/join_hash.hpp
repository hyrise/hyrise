#pragma once

#include <algorithm>
#include <atomic>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef WITH_PAPI
#include <papi.h>
#endif

#include "abstract_join_operator.hpp"

#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "type_comparison.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/cuckoo_hashtable.hpp"
#include "utils/murmur_hash.hpp"

namespace opossum {

/**
 * This operator joins two tables using one column of each table.
 * The output is a new table with referenced columns for all columns of the two inputs and filtered pos_lists.
 * If you want to filter by multiple criteria, you can chain this operator.
 *
 * As with most operators, we do not guarantee a stable operation with regards to positions -
 * i.e., your sorting order might be disturbed.
 *
 * Note: JoinHash does not support null values at the moment
 *
 * Find more information in our Wiki: https://github.com/hyrise/zweirise/wiki/Radix-Partitioned-and-Hash-Based-Join
 */
class JoinHash : public AbstractJoinOperator {
 public:
  JoinHash(const std::shared_ptr<const AbstractOperator> left, const std::shared_ptr<const AbstractOperator> right,
           const JoinMode mode, const std::pair<ColumnID, ColumnID> &column_ids, const ScanType scan_type);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override {
    Fail("Operator " + this->name() + " does not implement recreation.");
    return {};
  }

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;

  template <typename LeftType, typename RightType>
  class JoinHashImpl;
};

}  // namespace opossum
