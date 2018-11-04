#include "like_matcher.hpp"

#include "boost/algorithm/string/replace.hpp"

#include "utils/assert.hpp"

namespace opossum {

LikeMatcher::LikeMatcher(const std::string& pattern) { _pattern_variant = pattern_string_to_pattern_variant(pattern); }

size_t LikeMatcher::get_index_of_next_wildcard(const std::string& pattern, const size_t offset) {
  return pattern.find_first_of("_%", offset);
}

bool LikeMatcher::contains_wildcard(const std::string& pattern) {
  return get_index_of_next_wildcard(pattern) != std::string::npos;
}

LikeMatcher::PatternTokens LikeMatcher::pattern_string_to_tokens(const std::string& pattern) {
  PatternTokens tokens;

  auto current_position = size_t{0};
  while (current_position < pattern.size()) {
    if (pattern[current_position] == '_') {
      tokens.emplace_back(Wildcard::SingleChar);
      ++current_position;
    } else if (pattern[current_position] == '%') {
      tokens.emplace_back(Wildcard::AnyChars);
      ++current_position;
    } else {
      const auto next_wildcard_position = get_index_of_next_wildcard(pattern, current_position);
      const auto token_length =
          next_wildcard_position == std::string::npos ? std::string::npos : next_wildcard_position - current_position;
      tokens.emplace_back(pattern.substr(current_position, token_length));
      current_position = next_wildcard_position;
    }
  }

  return tokens;
}

LikeMatcher::AllPatternVariant LikeMatcher::pattern_string_to_pattern_variant(const std::string& pattern) {
  const auto tokens = pattern_string_to_tokens(pattern);

  if (tokens.size() == 2 && tokens[0].type() == typeid(std::string) && tokens[1] == PatternToken{Wildcard::AnyChars}) {
    // Pattern has the form 'hello%'
    return StartsWithPattern{boost::get<std::string>(tokens[0])};

  } else if (tokens.size() == 2 && tokens[0] == PatternToken{Wildcard::AnyChars} &&  // NOLINT
             tokens[1].type() == typeid(std::string)) {
    // Pattern has the form '%hello'
    return EndsWithPattern{boost::get<std::string>(tokens[1])};

  } else if (tokens.size() == 3 && tokens[0] == PatternToken{Wildcard::AnyChars} &&  // NOLINT
             tokens[1].type() == typeid(std::string) && tokens[2] == PatternToken{Wildcard::AnyChars}) {
    // Pattern has the form '%hello%'
    return ContainsPattern{boost::get<std::string>(tokens[1])};

  } else {
    /**
     * Pattern is either MultipleContainsPattern, e.g., '%hello%world%how%are%you%' or, if it isn't we fall back to
     * using a regex matcher.
     *
     * A MultipleContainsPattern begins and ends with '%' and  contains only strings and '%'.
     */

    // Pick ContainsMultiple or Regex
    auto pattern_is_contains_multiple = true;   // Set to false if tokens don't match %(, string, %)* pattern
    auto strings = std::vector<std::string>{};  // arguments used for ContainsMultiple, if it gets used
    auto expect_any_chars = true;               // If true, expect '%', if false, expect a string

    // Check if the tokens match the layout expected for MultipleContainsPattern - or break and set
    // pattern_is_contains_multiple to false once they don't
    for (const auto& token : tokens) {
      if (expect_any_chars && token != PatternToken{Wildcard::AnyChars}) {
        pattern_is_contains_multiple = false;
        break;
      }
      if (!expect_any_chars && token.type() != typeid(std::string)) {
        pattern_is_contains_multiple = false;
        break;
      }
      if (!expect_any_chars) {
        strings.emplace_back(boost::get<std::string>(token));
      }

      expect_any_chars = !expect_any_chars;
    }

    if (pattern_is_contains_multiple) {
      return MultipleContainsPattern{strings};
    } else {
      return std::regex(sql_like_to_regex(pattern));
    }
  }
}

std::string LikeMatcher::sql_like_to_regex(std::string sql_like) {
  // Do substitution of <backslash> with <backslash><backslash> FIRST, because otherwise it will also replace
  // backslashes introduced by the other substitutions
  constexpr auto REPLACE_BY = std::array<std::pair<const char*, const char*>, 14u>{{{"\\", "\\\\"},
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

  return "^" + sql_like + "$";
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

}  // namespace opossum
