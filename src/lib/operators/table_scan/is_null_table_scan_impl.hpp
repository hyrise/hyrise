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
  IsNullTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID base_column_id,
                      const PredicateCondition& predicate_condition);

  void handle_column(const ReferenceColumn& base_column, std::shared_ptr<ColumnVisitorContext> base_context) override;

  void handle_column(const BaseValueColumn& base_column, std::shared_ptr<ColumnVisitorContext> base_context) override;

  void handle_column(const BaseDictionaryColumn& base_column,
                     std::shared_ptr<ColumnVisitorContext> base_context) override;

  void handle_column(const BaseEncodedColumn& base_column, std::shared_ptr<ColumnVisitorContext> base_context) override;

  using BaseSingleColumnTableScanImpl::handle_column;

 private:
  /**
   * @defgroup Methods used for handling value columns
   * @{
   */

  bool _matches_all(const BaseValueColumn& column);

  bool _matches_none(const BaseValueColumn& column);

  void _add_all(Context& context, size_t column_size);

  /**@}*/

 private:
  template <typename Functor>
  void _resolve_predicate_condition(const Functor& func) {
    switch (_predicate_condition) {
      case PredicateCondition::IsNull:
        return func([](const bool is_null) { return is_null; });

      case PredicateCondition::IsNotNull:
        return func([](const bool is_null) { return !is_null; });

      default:
        Fail("Unsupported comparison type encountered");
    }
  }

  template <typename Iterator>
  void _scan(Iterator left_it, Iterator left_end, Context& context) {
    auto& matches_out = context._matches_out;
    const auto chunk_id = context._chunk_id;

    _resolve_predicate_condition([&](auto comparator) {
      for (; left_it != left_end; ++left_it) {
        const auto left = *left_it;

        if (!comparator(left.is_null())) continue;
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    });
  }
};

}  // namespace opossum
