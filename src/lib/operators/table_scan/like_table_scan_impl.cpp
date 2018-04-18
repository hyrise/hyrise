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

#include "storage/column_iterables/constant_value_iterable.hpp"
#include "storage/column_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/resolve_encoded_column_type.hpp"
#include "storage/value_column.hpp"
#include "storage/value_column/value_column_iterable.hpp"

namespace opossum {

LikeTableScanImpl::LikeTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                     const PredicateCondition predicate_condition, const std::string& right_wildcard)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, predicate_condition},
      _right_wildcard{right_wildcard},
      _invert_results(predicate_condition == PredicateCondition::NotLike) {
  // convert the given SQL-like search term into a c++11 regex to use it for the actual matching
  auto regex_string = sqllike_to_regex(_right_wildcard);
  _regex = std::regex{regex_string};  // case insensitivity
  const auto tokens = pattern_to_tokens(right_wildcard);
}

void LikeTableScanImpl::handle_column(const BaseValueColumn& base_column,
                                      std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  auto& left_column = static_cast<const ValueColumn<std::string>&>(base_column);

  auto left_iterable = ValueColumnIterable<std::string>{left_column};
  auto right_iterable = ConstantValueIterable<std::regex>{_regex};

  const auto regex_match = [this](const std::string& str) { return tokens_match_string(_tokens, str) ^ _invert_results; };
  left_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    this->_unary_scan(regex_match, left_it, left_end, chunk_id, matches_out);
  });
}

void LikeTableScanImpl::handle_column(const BaseEncodedColumn& base_column,
                                      std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  resolve_encoded_column_type<std::string>(base_column, [&](const auto& typed_column) {
    auto left_iterable = create_iterable_from_column(typed_column);
    auto right_iterable = ConstantValueIterable<std::regex>{_regex};

    const auto regex_match = [this](const std::string& str) { return std::regex_match(str, _regex) ^ _invert_results; };

    left_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      this->_unary_scan(regex_match, left_it, left_end, chunk_id, matches_out);
    });
  });
}

std::string LikeTableScanImpl::sqllike_to_regex(std::string sqllike) {
  // Do substitution of <backslash> with <backslash><backslash> FIRST, because otherwise it will also replace
  // backslashes introduced by the other substitutions
  constexpr auto replace_by = std::array<std::pair<const char*, const char*>, 15u>{{{"\\", "\\\\"},
                                                                                    {".", "\\."},
                                                                                    {"^", "\\^"},
                                                                                    {"$", "\\$"},
                                                                                    {"+", "\\+"},
                                                                                    {"?", "\\?"},
                                                                                    {"(", "\\("},
                                                                                    {")", "\\)"},
                                                                                    {"{", "\\{"},
                                                                                    {"}", "\\}"},
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

LikeTableScanImpl::PatternTokens LikeTableScanImpl::pattern_to_tokens(const std::string& pattern) {
  PatternTokens tokens;

  auto current_position = size_t{0};
  while (current_position < pattern.size()) {
    if (pattern[current_position] == '_') {
      tokens.emplace_back(PatternWildcard::SingleChar);
      ++current_position;
    } else if (pattern[current_position] == '%') {
      tokens.emplace_back(PatternWildcard::AnyChars);
      ++current_position;
    } else {
      const auto next_wildcard_position = pattern.find_first_of("_%", current_position);
      const auto token_length = next_wildcard_position == std::string::npos ? std::string::npos : next_wildcard_position - current_position;
      tokens.emplace_back(pattern.substr(current_position, token_length));
      current_position = next_wildcard_position;
    }
  }

  return tokens;
}

bool LikeTableScanImpl::tokens_match_string(const PatternTokens &tokens, const std::string &str) {
  enum class MatchMode {
    RightHere,    // Match the current token precisely at the current_position
    StartingHere  // Match the current token anywhere starting from the current_position
  };

  auto match_mode = MatchMode::RightHere;

  auto token_idx = size_t{0};
  auto current_position = size_t{0};

  for (; token_idx < tokens.size(); ++token_idx) {
    const auto& token = tokens[token_idx];

    if (token.which() == 1) {  // Token is Wildcard
      if (boost::get<PatternWildcard>(token) == PatternWildcard::SingleChar) {
        if (current_position >= str.size()) return false;
        ++current_position;
      } else {
        match_mode = MatchMode::StartingHere;
      }
    } else {  // Token is std::string
      if (current_position >= str.size()) return false;

      const auto string_token = boost::get<std::string>(token);

      if (match_mode == MatchMode::RightHere) {
        if (str.compare(current_position, string_token.size(), string_token) != 0) return false;
        current_position += string_token.size();
      } else {
        current_position = str.find(string_token, current_position);
        if (current_position == std::string::npos) return false;
        current_position += string_token.size();
      }

      match_mode = MatchMode::RightHere;
    }
  }

  return (current_position == str.size() || match_mode == MatchMode::StartingHere) && token_idx == tokens.size();
}

void LikeTableScanImpl::handle_column(const BaseDictionaryColumn& base_column,
                                      std::shared_ptr<ColumnVisitableContext> base_context) {
  const auto& left_column = static_cast<const DictionaryColumn<std::string>&>(base_column);
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  const auto result = _find_matches_in_dictionary(*left_column.dictionary());
  const auto& match_count = result.first;
  const auto& dictionary_matches = result.second;

  auto attribute_vector_iterable = create_iterable_from_attribute_vector(left_column);

  // Regex matches all
  if (match_count == dictionary_matches.size()) {
    attribute_vector_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      static const auto always_true = [](const auto&) { return true; };
      this->_unary_scan(always_true, left_it, left_end, chunk_id, matches_out);
    });

    return;
  }

  // Regex mathes none
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

}  // namespace opossum
