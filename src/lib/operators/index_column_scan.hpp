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

// operator to filter a table by a single attribute
// output is a table with only reference columns
// to filter by multiple criteria, you can chain the operator

// As with most operators, we do not guarantee a stable operation with regards to positions - i.e., your sorting order
// might be disturbed

// This scan differs from the normal table_scan in the single fact that it uses an index on the column to scan
// if there exists one
// Therefore, 95% of this code is duplicate to the table_scan.hpp
// Ideas on how to overcome this duplication are welcome

class IndexColumnScan : public AbstractReadOnlyOperator {
 public:
  IndexColumnScan(const std::shared_ptr<AbstractOperator> in, const std::string &filter_column_name,
                  const std::string &op, const AllTypeVariant value, const optional<AllTypeVariant> value2 = nullopt);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  inline std::shared_ptr<AbstractOperator> recreate() const override {
    throw std::runtime_error("Operator " + this->name() + " does not implement recreation.");
  }

 protected:
  std::shared_ptr<const Table> on_execute() override;

  template <typename T>
  class IndexColumnScanImpl;

  const std::string _column_name;
  const std::string _op;
  const AllTypeVariant _value;
  const optional<AllTypeVariant> _value2;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;

  enum ScanType { OpEquals, OpNotEquals, OpLessThan, OpLessThanEquals, OpGreaterThan, OpGreaterThanEquals, OpBetween };
};

}  // namespace opossum
