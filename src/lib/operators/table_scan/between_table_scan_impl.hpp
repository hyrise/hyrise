#pragma once

#include <memory>

#include "base_single_column_table_scan_impl.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class Table;

/**
 * @brief Compares a column to two scalar values (... WHERE col BETWEEN left_value and right_value)
 *
 * Limitations:
 * - We expect left_value and right_value to be scalar values, not columns
 * - They are also expected to have the same data type
 *
 * Both of these limitations are to keep the code complexity and the number of template instantiations low,
 * more complicated cases are handled by two scans, see operator_scan_predicate.cpp
 */
class BetweenTableScanImpl : public BaseSingleColumnTableScanImpl {
 public:
  BetweenTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID left_column_id,
                       const AllTypeVariant& left_value, const AllTypeVariant& right_value);

  std::string description() const override;

  void handle_segment(const BaseValueSegment& base_segment,
                      std::shared_ptr<SegmentVisitorContext> base_context) override;

  void handle_segment(const BaseDictionarySegment& base_segment,
                      std::shared_ptr<SegmentVisitorContext> base_context) override;

  void handle_segment(const BaseEncodedSegment& base_segment,
                      std::shared_ptr<SegmentVisitorContext> base_context) override;

  using BaseSingleColumnTableScanImpl::handle_segment;

 private:
  const AllTypeVariant _left_value;
  const AllTypeVariant _right_value;
};

}  // namespace opossum
