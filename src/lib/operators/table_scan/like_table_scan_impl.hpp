#pragma once

#include <map>
#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "base_single_column_table_scan_impl.hpp"
#include "boost/variant.hpp"

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

  /**
   * Turn SQL LIKE-pattern into a C++ regex.
   */
  static std::string sql_like_to_regex(std::string sql_like);

  enum class Wildcard { SingleChar /* '_' */, AnyChars /* '%' */ };
  using PatternToken = boost::variant<std::string, Wildcard>;  // Keep type order, users rely on which()
  using PatternTokens = std::vector<PatternToken>;

  /**
   * Turn a pattern string, e.g. "H_llo W%ld" into Tokens {"H", PatternWildcard::SingleChar, "llo W",
   * PatternWildcard::AnyChars, "ld"}
   */
  static PatternTokens pattern_string_to_tokens(const std::string& pattern);

  /**
   * To speed up LIKE there are special implementations available for simple, common patterns.
   * Any other pattern will fall back to regex.
   */
  // 'hello%'
  struct StartsWithPattern final {
    std::string string;
  };
  // '%hello'
  struct EndsWithPattern final {
    std::string string;
  };
  // '%hello%'
  struct ContainsPattern final {
    std::string string;
  };
  // '%hello%world%nice%weather%'
  struct MultipleContainsPattern final {
    std::vector<std::string> strings;
  };

  /**
   * Contains one of the specialised patterns from above (StartsWithPattern, ...) or falls back to std::regex for a
   * general pattern.
   */
  using AllPatternVariant =
      boost::variant<std::regex, StartsWithPattern, EndsWithPattern, ContainsPattern, MultipleContainsPattern>;

  static AllPatternVariant pattern_string_to_pattern_variant(const std::string& pattern);

  /**
   * Call functor with the resolved Pattern
   */
  template <typename Functor>
  static void resolve_pattern_matcher(const AllPatternVariant& pattern_variant, const bool invert_results,
                                      const Functor& functor);

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

  const std::string _pattern;

  // For NOT LIKE support
  const bool _invert_results;

  AllPatternVariant _pattern_variant;
};

std::ostream& operator<<(std::ostream& stream, const LikeTableScanImpl::Wildcard& wildcard);

}  // namespace opossum
