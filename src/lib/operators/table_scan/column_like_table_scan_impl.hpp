#pragma once

#include <map>
#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "abstract_dereferenced_column_table_scan_impl.hpp"
#include "boost/variant.hpp"
#include "expression/evaluation/like_matcher.hpp"

#include "types.hpp"

namespace opossum {

class Table;

/**
 * @brief Implements a column scan using the LIKE operator
 *
 * - The only supported type is pmr_string.
 * - Value segments are scanned sequentially
 * - For dictionary segments, we check the values in the dictionary and store the matches in a vector
 *   in order to avoid having to look up each value ID of the attribute vector in the dictionary. This also
 *   enables us to detect if all or none of the values in the segment satisfy the expression.
 *
 * Performance Notes: Uses std::regex as a slow fallback and resorts to much faster Pattern matchers for special cases,
 *                    e.g., StartsWithPattern. 
 */
class ColumnLikeTableScanImpl : public AbstractDereferencedColumnTableScanImpl {
 public:
  ColumnLikeTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                          const PredicateCondition init_predicate_condition, const pmr_string& pattern);

  std::string description() const override;

 protected:
  void _scan_non_reference_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
                                   const std::shared_ptr<const PosList>& position_filter) const override;

  void _scan_generic_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
                             const std::shared_ptr<const PosList>& position_filter) const;
  void _scan_dictionary_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id, PosList& matches,
                                const std::shared_ptr<const PosList>& position_filter) const;

  /**
   * Used for dictionary segments
   * @returns number of matches and the result of each dictionary entry
   */
  template <typename D>
  std::pair<size_t, std::vector<bool>> _find_matches_in_dictionary(const D& dictionary) const;

  const LikeMatcher _matcher;

  // For NOT LIKE support
  const bool _invert_results;
};

}  // namespace opossum
