#pragma once

#include <map>
#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "boost/variant.hpp"
#include "base_single_column_table_scan_impl.hpp"

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
                    const PredicateCondition predicate_condition, const std::string& right_wildcard);

  void handle_column(const BaseValueColumn& base_column, std::shared_ptr<ColumnVisitableContext> base_context) override;

  void handle_column(const BaseDictionaryColumn& base_column,
                     std::shared_ptr<ColumnVisitableContext> base_context) override;

  void handle_column(const BaseEncodedColumn& base_column,
                     std::shared_ptr<ColumnVisitableContext> base_context) override;

  using BaseSingleColumnTableScanImpl::handle_column;

 public:
  /**
   * @defgroup Methods which are used to convert an SQL wildcard into a C++ regex.
   * @{
   */

  static std::string sqllike_to_regex(std::string sqllike);

  /**@}*/

  enum class PatternWildcard { SingleChar /* '_' */, AnyChars /* '%' */ };
  using PatternToken = boost::variant<std::string, PatternWildcard>; // Keep type order, users rely on which()
  using PatternTokens = std::vector<PatternToken>;

  /**
   * Turn a pattern string, e.g. "H_llo W%ld" into Tokens {"H", PatternWildcard::SingleChar, "llo W",
   * PatternWildcard::AnyChars, "ld"}
   */
  static PatternTokens pattern_to_tokens(const std::string& pattern);

  /**
   * Check whether a string matches a series of tokens.
   */
  static bool tokens_match_string(const PatternTokens &tokens, const std::string &str);

  private:
  /**
   * @defgroup Methods used for handling dictionary columns
   * @{
   */

  /**
   * @returns number of matches and the result of each dictionary entry
   */
  std::pair<size_t, std::vector<bool>> _find_matches_in_dictionary(const pmr_vector<std::string>& dictionary);

  /**@}*/

 private:
  const std::string _right_wildcard;
  const bool _invert_results;

  PatternTokens _tokens;

  std::regex _regex;
};

inline std::ostream& operator<<(std::ostream& stream, const LikeTableScanImpl::PatternWildcard& wildcard) {
  stream << "[WC]";
  return stream;
}

}  // namespace opossum
