#pragma once

#include <memory>

#include "types.hpp"

#include "storage/iterables/dictionary_column_iterable.hpp"
#include "storage/iterables/reference_column_iterable.hpp"
#include "storage/iterables/value_column_iterable.hpp"

namespace opossum {

class Table;

class BaseTableScanImpl {
 public:
  BaseTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType scan_type)
      : _in_table{in_table}, _left_column_id{left_column_id}, _scan_type{scan_type} {}

  virtual ~BaseTableScanImpl() = default;

  virtual PosList scan_chunk(const ChunkID &chunk_id) = 0;

 protected:
  template <typename Type>
  static auto _create_iterable_from_column(ValueColumn<Type> &column) {
    return ValueColumnIterable<Type>{column};
  }

  template <typename Type>
  static auto _create_iterable_from_column(DictionaryColumn<Type> &column) {
    return DictionaryColumnIterable<Type>{column};
  }

  template <typename Type>
  static auto _create_iterable_from_column(ReferenceColumn &column) {
    return ReferenceColumnIterable<Type>{column};
  }

 protected:
  const std::shared_ptr<const Table> _in_table;
  const ColumnID _left_column_id;
  const ScanType _scan_type;
};

}  // namespace opossum