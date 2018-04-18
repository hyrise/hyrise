#pragma once

#include <map>
#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "boost/variant.hpp"
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
 */
class LikeTableScanImpl : public BaseSingleColumnTableScanImpl {
 public:
  LikeTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                    const PredicateCondition predicate_condition, const std::string& pattern);

  void handle_column(const BaseValueColumn& base_column, std::shared_ptr<ColumnVisitableContext> base_context) override;

  void handle_column(const BaseDictionaryColumn& base_column,
                     std::shared_ptr<ColumnVisitableContext> base_context) override;

  void handle_column(const BaseEncodedColumn& base_column,
                     std::shared_ptr<ColumnVisitableContext> base_context) override;

  using BaseSingleColumnTableScanImpl::handle_column;

  /**
   * Turn SQL LIKE-pattern into a C++ regex.
   */
  static std::string sql_like_to_regex(std::string sqllike);

  enum class PatternWildcard { SingleChar /* '_' */, AnyChars /* '%' */ };
  using PatternToken = boost::variant<std::string, PatternWildcard>; // Keep type order, users rely on which()
  using PatternTokens = std::vector<PatternToken>;

  /**
   * Turn a pattern string, e.g. "H_llo W%ld" into Tokens {"H", PatternWildcard::SingleChar, "llo W",
   * PatternWildcard::AnyChars, "ld"}
   */
  static PatternTokens pattern_string_to_tokens(const std::string &pattern);

  /**
   * To speed up LIKE there are special implementations available for simple, common patterns.
   * Any other pattern will fall back to regex.
   */
  struct StartsWithPattern final { std::string str; };  // 'hello%'
  struct EndsWithPattern final { std::string str; };  // '%hello'
  struct ContainsPattern final { std::string str; };  // '%hello%'
  struct ContainsMultiplePattern final { std::vector<std::string> str; };  // '%hello%world%nice%wheather'

  using PatternVariant = boost::variant<std::regex, StartsWithPattern, EndsWithPattern, ContainsPattern, ContainsMultiplePattern>;

  static PatternVariant pattern_string_to_pattern_variant(const std::string &pattern);

  template<typename Functor>
  static void resolve_pattern_matcher(const PatternVariant &pattern_variant, const bool invert, const Functor &functor);

 private:
  template<typename Iterable>
  void _scan_iterable(const Iterable& iterable,
                      const ChunkID chunk_id,
                      PosList& matches_out,
                     const ChunkOffsetsList* const mapped_chunk_offsets);

  /**
   * Used for dictionary columns
   * @returns number of matches and the result of each dictionary entry
   */
  std::pair<size_t, std::vector<bool>> _find_matches_in_dictionary(const pmr_vector<std::string>& dictionary);

  const std::string _pattern;
  const bool _invert_results;

  PatternVariant _pattern_variant;
};

std::ostream& operator<<(std::ostream& stream, const LikeTableScanImpl::PatternWildcard& wildcard);

}  // namespace opossum
