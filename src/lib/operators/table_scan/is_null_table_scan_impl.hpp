#pragma once

#include <functional>
#include <memory>

#include "base_single_column_table_scan_impl.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;
class BaseValueColumn;

class IsNullTableScanImpl : public BaseSingleColumnTableScanImpl {
 public:
  IsNullTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType &scan_type);

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override;

  void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override;

 private:
  /**
   * @defgroup Methods used for handling value columns
   * @{
   */

  bool _matches_all(const BaseValueColumn &column);

  bool _matches_none(const BaseValueColumn &column);

  void _add_all(Context &context, size_t column_size);

  /**@}*/

 private:
  template <typename Functor>
  void _resolve_scan_type(const Functor &func) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        return func([](const bool is_null) { return is_null; });

      case ScanType::OpNotEquals:
        return func([](const bool is_null) { return !is_null; });

      default:
        Fail("Unsupported comparison type encountered");
    }
  }

  template <typename Iterator>
  void _scan(Iterator left_it, Iterator left_end, Context &context) {
    auto &matches_out = context._matches_out;
    const auto chunk_id = context._chunk_id;

    _resolve_scan_type([&](auto comparator) {
      for (; left_it != left_end; ++left_it) {
        const auto left = *left_it;

        if (!comparator(left.is_null())) continue;
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    });
  }
};

}  // namespace opossum
