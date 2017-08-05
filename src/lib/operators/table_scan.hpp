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

class AbstractScan;

class TableScan : public AbstractReadOnlyOperator {
 public:
  TableScan(const std::shared_ptr<AbstractOperator> in, const std::string &left_column_name,
               const ScanType scan_type, const AllParameterVariant right_parameter,
               const optional<AllTypeVariant> right_value2 = nullopt);

  ~TableScan();

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  void init_scan();
  void init_output_table();

 private:
  const std::string _left_column_name;
  const ScanType _scan_type;
  const AllParameterVariant _right_parameter;
  const optional<AllTypeVariant> _right_value2;

  std::shared_ptr<const Table> _in_table;
  bool _is_reference_table;
  std::unique_ptr<AbstractScan> _scan;
  std::shared_ptr<Table> _output_table;
};

}  // namespace opossum
