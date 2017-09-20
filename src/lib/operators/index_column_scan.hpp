#pragma once

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"

#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/index/base_index.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * operator to filter a table by a single attribute
 * output is a table with only reference columns
 * to filter by multiple criteria, you can chain the operator
 *
 * As with most operators, we do not guarantee a stable operation with regards to positions - i.e., your sorting order
 * might be disturbed

 * This scan differs from the normal table_scan in the single fact that it uses an index on the column to scan
 * if there exists one
 * Therefore, 95% of this code is duplicate to the table_scan.hpp
 * Ideas on how to overcome this duplication are welcome
 *
 * Note: IndexColumnScan does not support null values at the moment
 */
class IndexColumnScan : public AbstractReadOnlyOperator {
 public:
  IndexColumnScan(const std::shared_ptr<AbstractOperator> in, const ColumnID column_id, const ScanType scan_type,
                  const AllTypeVariant value, const optional<AllTypeVariant> value2 = nullopt);

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

  template <typename T>
  class IndexColumnScanImpl;

  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _value;
  const optional<AllTypeVariant> _value2;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;
};

}  // namespace opossum
