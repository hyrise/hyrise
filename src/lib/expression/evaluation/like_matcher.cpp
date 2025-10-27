#include "like_matcher.hpp"

#include <array>
#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string/replace.hpp>

#include "types.hpp"
#include "utils/string_utils.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

size_t get_index_of_next_wildcard(const pmr_string& pattern, const size_t offset) {
  return pattern.find_first_of("_%", offset);
}

bool contains_wildcard(const pmr_string& pattern) {
  return get_index_of_next_wildcard(pattern, 0) != pmr_string::npos;
}

std::string sql_like_to_regex(pmr_string sql_like) {
  // Do substitution of <backslash> with <backslash><backslash> FIRST, because otherwise it will also replace
  // backslashes introduced by the other substitutions
  constexpr auto REPLACE_BY = std::array<std::pair<const char*, const char*>, 14>{{{"\\", "\\\\"},
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
                                                                                   {"*", "\\*"},
                                                                                   {"%", ".*"},
                                                                                   {"_", "."}}};

  for (const auto& pair : REPLACE_BY) {
    boost::replace_all(sql_like, pair.first, pair.second);
  }

  return std::string{"^" + sql_like + "$"};
}

}  // namespace

namespace hyrise {

LikeMatcher::LikeMatcher(const pmr_string& pattern, const PredicateCondition predicate_condition)
    : _predicate_condition{predicate_condition} {
  Assert(_predicate_condition == PredicateCondition::Like || _predicate_condition == PredicateCondition::NotLike ||
             _predicate_condition == PredicateCondition::LikeInsensitive ||
             _predicate_condition == PredicateCondition::NotLikeInsensitive,
         "Expected PredicateCondition (Not)Like or (Not)LikeInsensitive.");
  const auto case_insensitive = _predicate_condition == PredicateCondition::LikeInsensitive ||
                                _predicate_condition == PredicateCondition::NotLikeInsensitive;
  _pattern_variant = pattern_string_to_pattern_variant(pattern, case_insensitive);
}

LikeMatcher::PatternTokens LikeMatcher::pattern_string_to_tokens(const pmr_string& pattern) {
  auto tokens = PatternTokens{};

  auto current_position = size_t{0};
  const auto pattern_length = pattern.size();
  while (current_position < pattern_length) {
    if (pattern[current_position] == '_') {
      tokens.emplace_back(Wildcard::SingleChar);
      ++current_position;
    } else if (pattern[current_position] == '%') {
      tokens.emplace_back(Wildcard::AnyChars);
      ++current_position;
    } else {
      const auto next_wildcard_position = get_index_of_next_wildcard(pattern, current_position);
      const auto token_length =
          next_wildcard_position == pmr_string::npos ? pmr_string::npos : next_wildcard_position - current_position;
      tokens.emplace_back(pattern.substr(current_position, token_length));
      current_position = next_wildcard_position;
    }
  }

  return tokens;
}

LikeMatcher::AllPatternVariant LikeMatcher::pattern_string_to_pattern_variant(const pmr_string& pattern,
                                                                              const bool case_insensitive) {
  const auto cased_pattern = case_insensitive ? string_to_lower(pattern) : pattern;
  const auto tokens = pattern_string_to_tokens(cased_pattern);

  if (tokens.size() == 2 && std::holds_alternative<pmr_string>(tokens[0]) &&
      tokens[1] == PatternToken{Wildcard::AnyChars}) {
    // Pattern has the form 'hello%'.
    return StartsWithPattern{std::get<pmr_string>(tokens[0])};
  }

  if (tokens.size() == 2 && tokens[0] == PatternToken{Wildcard::AnyChars} &&
      std::holds_alternative<pmr_string>(tokens[1])) {
    // Pattern has the form '%hello'.
    return EndsWithPattern{std::get<pmr_string>(tokens[1])};
  }

  if (tokens.size() == 3 && tokens[0] == PatternToken{Wildcard::AnyChars} &&
      std::holds_alternative<pmr_string>(tokens[1]) && tokens[2] == PatternToken{Wildcard::AnyChars}) {
    // Pattern has the form '%hello%'.
    return ContainsPattern{std::get<pmr_string>(tokens[1])};
  }

  /**
       * Pattern is either MultipleContainsPattern, e.g., '%hello%world%how%are%you%' or we fall back to
       * using a regex matcher.
       *
       * A MultipleContainsPattern begins and ends with '%' and  contains only strings and '%'.
       */

  // Pick ContainsMultiple or regex.
  auto pattern_is_contains_multiple = true;  // Set to false if tokens do not match %(, string, %)* pattern.
  auto strings = std::vector<pmr_string>{};  // Arguments used for ContainsMultiple, if it gets used.
  auto expect_any_chars = true;              // If true, expect '%', if false, expect a string.

  // Check if the tokens match the layout expected for MultipleContainsPattern - or break and set
  // pattern_is_contains_multiple to false once they do not.
  for (const auto& token : tokens) {
    if (expect_any_chars && token != PatternToken{Wildcard::AnyChars}) {
      pattern_is_contains_multiple = false;
      break;
    }
    if (!expect_any_chars && !std::holds_alternative<pmr_string>(token)) {
      pattern_is_contains_multiple = false;
      break;
    }
    if (!expect_any_chars) {
      strings.emplace_back(std::get<pmr_string>(token));
    }

    expect_any_chars = !expect_any_chars;
  }

  if (pattern_is_contains_multiple) {
    return MultipleContainsPattern{strings};
  }

  return std::regex{sql_like_to_regex(cased_pattern), std::regex::optimize};
}

std::optional<std::pair<pmr_string, pmr_string>> LikeMatcher::bounds(const pmr_string& pattern) {
  if (!contains_wildcard(pattern)) {
    const auto upper_bound = pmr_string(pattern) + '\0';
    return std::pair<pmr_string, pmr_string>(pattern, upper_bound);
  }
  const auto wildcard_pos = get_index_of_next_wildcard(pattern, 0);
  if (wildcard_pos == 0) {
    return std::nullopt;
  }
  // Calculate lower bound of the search Pattern
  const auto lower_bound = pattern.substr(0, wildcard_pos);
  const auto last_character_of_lower_bound = lower_bound.back();

  // Calculate upper bound of the search pattern according to ASCII-table
  constexpr int MAX_ASCII_VALUE = 127;
  if (last_character_of_lower_bound >= MAX_ASCII_VALUE) {
    // current_character_value + 1 would overflow.
    return std::nullopt;
  }
  const auto next_ascii_character = static_cast<char>(last_character_of_lower_bound + 1);
  const auto upper_bound = lower_bound.substr(0, lower_bound.size() - 1) + next_ascii_character;

  return std::pair<pmr_string, pmr_string>(lower_bound, upper_bound);
}

std::ostream& operator<<(std::ostream& stream, const LikeMatcher::Wildcard& wildcard) {
  switch (wildcard) {
    case LikeMatcher::Wildcard::SingleChar:
      stream << "_";
      break;
    case LikeMatcher::Wildcard::AnyChars:
      stream << "%";
      break;
  }

  return stream;
}

}  // namespace hyrise
