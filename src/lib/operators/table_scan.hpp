#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "all_parameter_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractReadOnlyOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID left_column_id, const ScanType scan_type,
            const AllParameterVariant right_parameter, const std::optional<AllTypeVariant> right_value2 = std::nullopt);

  ~TableScan();

  ColumnID left_column_id() const;
  ScanType scan_type() const;
  const AllParameterVariant& right_parameter() const;
  const std::optional<AllTypeVariant>& right_value2() const;

  const std::string name() const override;
  const std::string description() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;

  void _init_scan();

  // TODO(anyone): This is only a very temporary solution! Whoever reads this first must replace it.
  std::shared_ptr<const Table> __on_execute_between();

 private:
  const ColumnID _left_column_id;
  const ScanType _scan_type;
  const AllParameterVariant _right_parameter;
  const std::optional<AllTypeVariant> _right_value2;

  std::shared_ptr<const Table> _in_table;
  std::unique_ptr<BaseTableScanImpl> _impl;
  std::shared_ptr<Table> _output_table;
};

}  // namespace opossum
