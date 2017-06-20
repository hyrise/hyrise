#pragma once

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "all_parameter_variant.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// operator to filter a table by a single attribute
// output is an table with only reference columns
// to filter by multiple criteria, you can chain the operator

// As with most operators, we do not guarantee a stable operation with regards to positions - i.e., your sorting order
// might be disturbed

// Because the BETWEEN operator needs 3 values, we didn't implemented the possibilty to scan on 3 columns, as this
// would increase the if-complexity in handle_column again.
// That's why we only accept a constant value for the second between-parameter.

class TableScan : public AbstractReadOnlyOperator {
 public:
  TableScan(const std::shared_ptr<AbstractOperator> in, const std::string &filter_column_name, const std::string &op,
            const AllParameterVariant value, const optional<AllTypeVariant> value2 = nullopt);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  std::shared_ptr<AbstractOperator> recreate() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  template <typename T>
  class TableScanImpl;

  const std::string _column_name;
  const std::string _op;
  const AllParameterVariant _value;
  const optional<AllTypeVariant> _value2;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;

  enum ScanType {
    OpEquals,
    OpNotEquals,
    OpLessThan,
    OpLessThanEquals,
    OpGreaterThan,
    OpGreaterThanEquals,
    OpBetween,
    OpLike
  };

  static std::string &replace_all(std::string &str, const std::string &old_value, const std::string &new_value);
  static std::map<std::string, std::string> extract_character_ranges(std::string &str);
  static std::string sqllike_to_regex(std::string sqllike);
};

}  // namespace opossum
