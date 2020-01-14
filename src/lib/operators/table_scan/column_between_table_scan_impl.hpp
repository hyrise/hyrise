#pragma once

#include <memory>

#include "abstract_dereferenced_column_table_scan_impl.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class Table;

/**
 * @brief Compares a column to two scalar values (... WHERE col BETWEEN left_value AND right_value)
 *
 * Limitations:
 * - We expect left_value and right_value to be scalar values, not columns
 * - They are also expected to have the same data type
 *
 * Both of these limitations are to keep the code complexity and the number of template instantiations low,
 * more complicated cases are handled by two scans, see operator_scan_predicate.cpp
 */
class ColumnBetweenTableScanImpl : public AbstractDereferencedColumnTableScanImpl {
 public:
  ColumnBetweenTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                             const AllTypeVariant& left_value, const AllTypeVariant& right_value,
                             PredicateCondition init_predicate_condition);

  std::string description() const override;

  const AllTypeVariant left_value;
  const AllTypeVariant right_value;

 protected:
  void _scan_non_reference_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
                                   const std::shared_ptr<const PosList>& position_filter) const override;

  void _scan_generic_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
                             const std::shared_ptr<const PosList>& position_filter) const;

  // Optimized scan on DictionarySegments
  void _scan_dictionary_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id, PosList& matches,
                                const std::shared_ptr<const PosList>& position_filter) const;
};

}  // namespace opossum
