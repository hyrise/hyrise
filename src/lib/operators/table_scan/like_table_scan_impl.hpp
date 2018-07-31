#pragma once

#include <map>
#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "base_single_column_table_scan_impl.hpp"
#include "boost/variant.hpp"
#include "expression/evaluation/like_matcher.hpp"

#include "types.hpp"

namespace opossum {

class Table;

/**
 * @brief Implements a column scan using the LIKE operator
 *
 * - The only supported type is std::string.
 * - Value columns are scanned sequentially
 * - For dictionary columns, we check the values in the dictionary and store the results in a vector
 *   in order to avoid having to look up each value ID of the attribute vector in the dictionary. This also
 *   enables us to detect if all or none of the values in the column satisfy the expression.
 *
 * Performance Notes: Uses std::regex as a slow fallback and resorts to much faster Pattern matchers for special cases,
 *                    e.g., StartsWithPattern. 
 */
class LikeTableScanImpl : public BaseSingleColumnTableScanImpl {
 public:
  LikeTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID left_column_id,
                    const PredicateCondition predicate_condition, const std::string& pattern);

  void handle_column(const BaseValueColumn& base_column, std::shared_ptr<ColumnVisitorContext> base_context) override;

  void handle_column(const BaseDictionaryColumn& base_column,
                     std::shared_ptr<ColumnVisitorContext> base_context) override;

  void handle_column(const BaseEncodedColumn& base_column, std::shared_ptr<ColumnVisitorContext> base_context) override;

  using BaseSingleColumnTableScanImpl::handle_column;

 private:
  /**
   * Scan the iterable (using the optional mapped_chunk_offsets) with _pattern_variant and fill the matches_out with
   * RowIDs that match the pattern.
   */
  template <typename Iterable>
  void _scan_iterable(const Iterable& iterable, const ChunkID chunk_id, PosList& matches_out,
                      const ChunkOffsetsList* const mapped_chunk_offsets);

  /**
   * Used for dictionary columns
   * @returns number of matches and the result of each dictionary entry
   */
  std::pair<size_t, std::vector<bool>> _find_matches_in_dictionary(const pmr_vector<std::string>& dictionary);

  const LikeMatcher _matcher;

  // For NOT LIKE support
  const bool _invert_results;
};

}  // namespace opossum
