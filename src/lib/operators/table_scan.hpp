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
            const AllParameterVariant right_parameter);

  ~TableScan();

  /**
   * If set, the specified chunks will not be scanned.
   */
  void set_excluded_chunk_ids(const std::vector<ChunkID>& chunk_ids);

  ColumnID left_column_id() const;
  ScanType scan_type() const;
  const AllParameterVariant& right_parameter() const;

  const std::string name() const override;
  const std::string description() const override;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args = {}) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;

  void _init_scan();
  void _create_job_and_schedule(const ChunkID chunk_id, std::mutex& output_mutex);

 private:
  const ColumnID _left_column_id;
  const ScanType _scan_type;
  const AllParameterVariant _right_parameter;

  std::vector<ChunkID> _excluded_chunk_ids;

  std::shared_ptr<const Table> _in_table;
  std::unique_ptr<BaseTableScanImpl> _impl;
  std::shared_ptr<Table> _output_table;
};

}  // namespace opossum
