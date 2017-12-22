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
                                     const ScanType scan_type, const std::string& right_wildcard)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, scan_type},
      _right_wildcard{right_wildcard},
      _invert_results(scan_type == ScanType::NotLike) {
  // convert the given SQL-like search term into a c++11 regex to use it for the actual matching
  auto regex_string = _sqllike_to_regex(_right_wildcard);
  _regex = std::regex{regex_string, std::regex_constants::icase};  // case insensitivity
}

void LikeTableScanImpl::handle_value_column(const BaseValueColumn& base_column,
                                            std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  auto& left_column = static_cast<const ValueColumn<std::string>&>(base_column);

  auto left_iterable = ValueColumnIterable<std::string>{left_column};
  auto right_iterable = ConstantValueIterable<std::regex>{_regex};

  const auto regex_match = [this](const std::string& str) { return std::regex_match(str, _regex) ^ _invert_results; };

  left_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    this->_unary_scan(regex_match, left_it, left_end, chunk_id, matches_out);
  });
}

void LikeTableScanImpl::handle_dictionary_column(const BaseDictionaryColumn& base_column,
                                                 std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  const auto& left_column = static_cast<const DictionaryColumn<std::string>&>(base_column);

  const auto result = _find_matches_in_dictionary(*left_column.dictionary());
  const auto& match_count = result.first;
  const auto& dictionary_matches = result.second;

  const auto& attribute_vector = *left_column.attribute_vector();
  auto attribute_vector_iterable = AttributeVectorIterable{attribute_vector};

  if (match_count == dictionary_matches.size()) {
    attribute_vector_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      static const auto always_true = [](const auto&) { return true; };
      this->_unary_scan(always_true, left_it, left_end, chunk_id, matches_out);
    });

    return;
  }

  if (match_count == 0u) {
    return;
  }

  const auto dictionary_lookup = [&dictionary_matches](const ValueID& value) { return dictionary_matches[value]; };

  attribute_vector_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    this->_unary_scan(dictionary_lookup, left_it, left_end, chunk_id, matches_out);
  });
}

std::pair<size_t, std::vector<bool>> LikeTableScanImpl::_find_matches_in_dictionary(
    const pmr_vector<std::string>& dictionary) {
  auto result = std::pair<size_t, std::vector<bool>>{};

  auto& count = result.first;
  auto& dictionary_matches = result.second;

  count = 0u;
  dictionary_matches.reserve(dictionary.size());

  for (const auto& value : dictionary) {
    const auto result = std::regex_match(value, _regex) ^ _invert_results;
    count += static_cast<size_t>(result);
    dictionary_matches.push_back(result);
  }

  return result;
}

std::string LikeTableScanImpl::_sqllike_to_regex(std::string sqllike) {
  constexpr auto replace_by = std::array<std::pair<const char*, const char*>, 15u>{{{".", "\\."},
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

  for (const auto& pair : replace_by) {
    boost::replace_all(sqllike, pair.first, pair.second);
  }

  return "^" + sqllike + "$";
}

}  // namespace opossum
