#include "like_table_scan_impl.hpp"

#include <boost/algorithm/string/replace.hpp>

#include <algorithm>
#include <array>
#include <map>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "storage/dictionary_column.hpp"
#include "storage/iterables/attribute_vector_iterable.hpp"
#include "storage/iterables/constant_value_iterable.hpp"
#include "storage/iterables/value_column_iterable.hpp"
#include "storage/value_column.hpp"

namespace opossum {

LikeTableScanImpl::LikeTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                     const std::string &right_wildcard)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, ScanType::OpLike}, _right_wildcard{right_wildcard} {
  // convert the given SQL-like search term into a c++11 regex to use it for the actual matching
  auto regex_string = _sqllike_to_regex(_right_wildcard);
  _regex = std::regex{regex_string, std::regex_constants::icase};  // case insentivity
}

void LikeTableScanImpl::handle_value_column(BaseColumn &base_column,
                                            std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto &matches_out = context->_matches_out;
  const auto &mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  auto &left_column = static_cast<const ValueColumn<std::string> &>(base_column);

  auto left_iterable = ValueColumnIterable<std::string>{left_column};
  auto right_iterable = ConstantValueIterable<std::regex>{_regex};

  const auto regex_match = [this](const std::string &str) { return std::regex_match(str, _regex); };

  left_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    this->_unary_scan(regex_match, left_it, left_end, chunk_id, matches_out);
  });
}

void LikeTableScanImpl::handle_dictionary_column(BaseColumn &base_column,
                                                 std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto &matches_out = context->_matches_out;
  const auto &mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  const auto &left_column = static_cast<const DictionaryColumn<std::string> &>(base_column);

  const auto result = _find_matches_in_dictionary(*left_column.dictionary());
  const auto &match_count = result.first;
  const auto &dictionary_matches = result.second;

  const auto &attribute_vector = *left_column.attribute_vector();
  auto attribute_vector_iterable = AttributeVectorIterable{attribute_vector};

  if (match_count == dictionary_matches.size()) {
    attribute_vector_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      static const auto always_true = [](const auto &) { return true; };
      this->_unary_scan(always_true, left_it, left_end, chunk_id, matches_out);
    });

    return;
  }

  if (match_count == 0u) {
    return;
  }

  const auto dictionary_lookup = [&dictionary_matches](const ValueID &value) { return dictionary_matches[value]; };

  attribute_vector_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    this->_unary_scan(dictionary_lookup, left_it, left_end, chunk_id, matches_out);
  });
}

std::pair<size_t, std::vector<bool>> LikeTableScanImpl::_find_matches_in_dictionary(
    const pmr_vector<std::string> &dictionary) {
  auto result = std::pair<size_t, std::vector<bool>>{};

  auto &count = result.first;
  auto &dictionary_matches = result.second;

  count = 0u;
  dictionary_matches.reserve(dictionary.size());

  for (const auto &value : dictionary) {
    const auto result = std::regex_match(value, _regex);
    count += static_cast<size_t>(result);
    dictionary_matches.push_back(result);
  }

  return result;
}

std::map<std::string, std::string> LikeTableScanImpl::_extract_character_ranges(std::string &str) {
  std::map<std::string, std::string> ranges;

  int rangeID = 0;
  std::string::size_type startPos = 0;
  std::string::size_type endPos = 0;

  while ((startPos = str.find("[", startPos)) != std::string::npos &&
         (endPos = str.find("]", startPos + 1)) != std::string::npos) {
    std::stringstream ss;
    ss << "[[" << rangeID << "]]";
    std::string chars = str.substr(startPos + 1, endPos - startPos - 1);
    str.replace(startPos, chars.size() + 2, ss.str());
    rangeID++;
    startPos += ss.str().size();

    boost::replace_all(chars, "[", "\\[");
    boost::replace_all(chars, "]", "\\]");
    ranges[ss.str()] = "[" + chars + "]";
  }

  int open = 0;
  std::string::size_type searchPos = 0;
  startPos = 0;
  endPos = 0;
  do {
    startPos = str.find("[", searchPos);
    endPos = str.find("]", searchPos);

    if (startPos == std::string::npos && endPos == std::string::npos) break;

    if (startPos < endPos || endPos == std::string::npos) {
      open++;
      searchPos = startPos + 1;
    } else {
      if (open <= 0) {
        str.replace(endPos, 1, "\\]");
        searchPos = endPos + 2;
      } else {
        open--;
        searchPos = endPos + 1;
      }
    }
  } while (searchPos < str.size());
  return ranges;
}

std::string LikeTableScanImpl::_sqllike_to_regex(std::string sqllike) {
  constexpr auto replace_by = std::array<std::pair<const char *, const char *>, 15u>{{{".", "\\."},
                                                                                      {"^", "\\^"},
                                                                                      {"$", "\\$"},
                                                                                      {"+", "\\+"},
                                                                                      {"?", "\\?"},
                                                                                      {"(", "\\("},
                                                                                      {")", "\\"},
                                                                                      {"{", "\\{"},
                                                                                      {"}", "\\}"},
                                                                                      {"\\", "\\\\"},
                                                                                      {"|", "\\|"},
                                                                                      {".", "\\."},
                                                                                      {"*", "\\*"},
                                                                                      {"%", ".*"},
                                                                                      {"_", "."}}};

  for (const auto &pair : replace_by) {
    boost::replace_all(sqllike, pair.first, pair.second);
  }

  std::map<std::string, std::string> ranges = _extract_character_ranges(sqllike);  // Escapes [ and ] where necessary
  for (auto &range : ranges) {
    boost::replace_all(sqllike, range.first, range.second);
  }

  return "^" + sqllike + "$";
}

}  // namespace opossum
