#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "base_single_column_table_scan_impl.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseDictionaryColumn;
class BaseNewDictionaryColumn;

/**
 * @brief Compares one column to a constant value
 *
 * - Value columns are scanned sequentially
 * - For dictionary columns, we basically look up the value ID of the constant value in the dictionary
 *   in order to avoid having to look up each value ID of the attribute vector in the dictionary. This also
 *   enables us to detect if all or none of the values in the column satisfy the expression.
 */
class SingleColumnTableScanImpl : public BaseSingleColumnTableScanImpl {
 public:
  SingleColumnTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                            const ScanType& scan_type, const AllTypeVariant& right_value);

  PosList scan_chunk(ChunkID) override;

  void handle_value_column(const BaseValueColumn& base_column,
                           std::shared_ptr<ColumnVisitableContext> base_context) override;

  void handle_dictionary_column(const BaseDictionaryColumn& base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override;

  void handle_encoded_column(const BaseEncodedColumn& base_column,
                             std::shared_ptr<ColumnVisitableContext> base_context) override;

 private:
  /**
   * @defgroup Methods used for handling dictionary columns
   * @{
   */

  // The following methods are templated for as long as two dictionary column implementations exist.

  template <typename BaseDictionaryColumnType>
  void _handle_dictionary_column(const BaseDictionaryColumnType& left_column,
                                 std::shared_ptr<ColumnVisitableContext> base_context);

  template <typename BaseDictionaryColumnType>
  ValueID _get_search_value_id(const BaseDictionaryColumnType& column) const;

  template <typename BaseDictionaryColumnType>
  bool _right_value_matches_all(const BaseDictionaryColumnType& column, const ValueID search_value_id) const;

  template <typename BaseDictionaryColumnType>
  bool _right_value_matches_none(const BaseDictionaryColumnType& column, const ValueID search_value_id) const;

  template <typename Functor>
  void _with_operator_for_dict_column_scan(const ScanType scan_type, const Functor& func) const {
    switch (scan_type) {
      case ScanType::OpEquals:
        func(std::equal_to<void>{});
        return;

      case ScanType::OpNotEquals:
        func(std::not_equal_to<void>{});
        return;

      case ScanType::OpLessThan:
      case ScanType::OpLessThanEquals:
        func(std::less<void>{});
        return;

      case ScanType::OpGreaterThan:
      case ScanType::OpGreaterThanEquals:
        func(std::greater_equal<void>{});
        return;

      default:
        Fail("Unsupported comparison type encountered");
        return;
    }
  }

  /**@}*/

 private:
  const AllTypeVariant _right_value;
};

}  // namespace opossum
