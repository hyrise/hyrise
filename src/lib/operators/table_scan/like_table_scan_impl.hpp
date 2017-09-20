#pragma once

#include <map>
#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <vector>

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
                    const std::string &right_wildcard);

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override;

  void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override;

 private:
  /**
   * @defgroup Methods used for handling dictionary columns
   * @{
   */

  /**
   * @returns number of matches and the result of each dictionary entry
   */
  std::pair<size_t, std::vector<bool>> _find_matches_in_dictionary(const pmr_vector<std::string> &dictionary);

  /**@}*/

 private:
  /**
   * @defgroup Methods which are used to convert an SQL wildcard into a C++ regex.
   * @{
   */

  static std::map<std::string, std::string> _extract_character_ranges(std::string &str);

  static std::string _sqllike_to_regex(std::string sqllike);

  /**@}*/

 private:
  const std::string _right_wildcard;

  std::regex _regex;
};

}  // namespace opossum
