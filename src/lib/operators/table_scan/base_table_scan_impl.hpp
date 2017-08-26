#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class Table;

class BaseTableScanImpl {
 public:
  BaseTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType scan_type)
      : _in_table{in_table}, _left_column_id{left_column_id}, _scan_type{scan_type} {}

  virtual ~BaseTableScanImpl() = default;

  virtual PosList scan_chunk(const ChunkID &chunk_id) = 0;

 protected:
  const std::shared_ptr<const Table> _in_table;
  const ColumnID _left_column_id;
  const ScanType _scan_type;
};

}  // namespace opossum
