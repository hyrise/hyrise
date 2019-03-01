#pragma once

#include <regex>
#include <string>
#include <vector>

#include "boost/variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Wraps an SQL LIKE pattern (e.g. "Hello%Wo_ld") which strings can be tested against.
 *
 * Performance optimizations exist for several simple patterns, such as "Hello%" - which is really just a starts_with()
 * check.
 */
class LikeMatcher {
 public:
  /**
   * Turn SQL LIKE-pattern into a C++ regex.
   */
  static std::string sql_like_to_regex(pmr_string sql_like);

  static size_t get_index_of_next_wildcard(const pmr_string& pattern, const size_t offset = 0);
  static bool contains_wildcard(const pmr_string& pattern);

  explicit LikeMatcher(const pmr_string& pattern);

  enum class Wildcard { SingleChar /* '_' */, AnyChars /* '%' */ };
  using PatternToken = boost::variant<pmr_string, Wildcard>;  // Keep type order, users rely on which()
  using PatternTokens = std::vector<PatternToken>;

  /**
   * Turn a pattern string, e.g. "H_llo W%ld" into Tokens {"H", PatternWildcard::SingleChar, "llo W",
   * PatternWildcard::AnyChars, "ld"}
   */
  static PatternTokens pattern_string_to_tokens(const pmr_string& pattern);

  /**
   * To speed up LIKE there are special implementations available for simple, common patterns.
   * Any other pattern will fall back to regex.
   */
  // 'hello%'
  struct StartsWithPattern final {
    pmr_string string;
  };
  // '%hello'
  struct EndsWithPattern final {
    pmr_string string;
  };
  // '%hello%'
  struct ContainsPattern final {
    pmr_string string;
  };
  // '%hello%world%nice%weather%'
  struct MultipleContainsPattern final {
    std::vector<pmr_string> strings;
  };

  /**
   * Contains one of the specialised patterns from above (StartsWithPattern, ...) or falls back to std::regex for a
   * general pattern.
   */
  using AllPatternVariant =
      boost::variant<std::regex, StartsWithPattern, EndsWithPattern, ContainsPattern, MultipleContainsPattern>;

  static AllPatternVariant pattern_string_to_pattern_variant(const pmr_string& pattern);

  /**
   * The functor will be called with a concrete matcher.
   * Usage example:
   *    LikeMatcher{"%hello%"}.resolve(false, [](const auto& matcher) {
   *        std::cout << matcher("He said hello!") << std::endl;
   *    }
   */
  template <typename Functor>
  void resolve(const bool invert_results, const Functor& functor) const {
    if (_pattern_variant.type() == typeid(StartsWithPattern)) {
      const auto& prefix = boost::get<StartsWithPattern>(_pattern_variant).string;
      functor([&](const pmr_string& string) -> bool {
        if (string.size() < prefix.size()) return invert_results;
        return (string.compare(0, prefix.size(), prefix) == 0) ^ invert_results;
      });

    } else if (_pattern_variant.type() == typeid(EndsWithPattern)) {
      const auto& suffix = boost::get<EndsWithPattern>(_pattern_variant).string;
      functor([&](const pmr_string& string) -> bool {
        if (string.size() < suffix.size()) return invert_results;
        return (string.compare(string.size() - suffix.size(), suffix.size(), suffix) == 0) ^ invert_results;
      });

    } else if (_pattern_variant.type() == typeid(ContainsPattern)) {
      const auto& contains_str = boost::get<ContainsPattern>(_pattern_variant).string;
      functor([&](const pmr_string& string) -> bool {
        return (string.find(contains_str) != pmr_string::npos) ^ invert_results;
      });

    } else if (_pattern_variant.type() == typeid(MultipleContainsPattern)) {
      const auto& contains_strs = boost::get<MultipleContainsPattern>(_pattern_variant).strings;

      functor([&](const pmr_string& string) -> bool {
        auto current_position = size_t{0};
        for (const auto& contains_str : contains_strs) {
          current_position = string.find(contains_str, current_position);
          if (current_position == pmr_string::npos) return invert_results;
          current_position += contains_str.size();
        }
        return !invert_results;
      });

    } else if (_pattern_variant.type() == typeid(std::regex)) {
      const auto& regex = boost::get<std::regex>(_pattern_variant);

      functor([&](const pmr_string& string) -> bool { return std::regex_match(string, regex) ^ invert_results; });

    } else {
      Fail("Pattern not implemented. Probably a bug.");
    }
  }

 private:
  AllPatternVariant _pattern_variant;
};

std::ostream& operator<<(std::ostream& stream, const LikeMatcher::Wildcard& wildcard);

}  // namespace opossum
