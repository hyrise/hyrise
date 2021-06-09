#pragma once

#include <experimental/functional>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "re2/re2.h"

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
  // A faster search algorithm than the typical byte-wise search if we can reuse the searcher
#ifdef __GLIBCXX__
  using Searcher = std::boyer_moore_searcher<pmr_string::const_iterator>;
#else
  using Searcher = std::experimental::boyer_moore_searcher<pmr_string::const_iterator>;
#endif

 public:
  // Turn SQL LIKE-pattern into a regex pattern string.
  static std::string sql_like_to_regex(pmr_string sql_like);

  static size_t get_index_of_next_wildcard(const pmr_string& pattern, const size_t offset = 0);
  static bool contains_wildcard(const pmr_string& pattern);

  explicit LikeMatcher(const pmr_string& pattern);

  enum class Wildcard { SingleChar /* '_' */, AnyChars /* '%' */ };
  using PatternToken = std::variant<pmr_string, Wildcard>;  // Keep type order, users rely on which()
  using PatternTokens = std::vector<PatternToken>;

  /**
   * Turn a pattern string, e.g. "H_llo W%ld" into Tokens {"H", PatternWildcard::SingleChar, "llo W",
   * PatternWildcard::AnyChars, "ld"}
   */
  static PatternTokens pattern_string_to_tokens(const pmr_string& pattern);

  // Calculates the upper and lower bound of a given pattern. For example, with the pattern `Japan%`, the lower bound
  // `Japan` and upper bound `Japao` is returned. The first value of the returned pair is the lower bound, the second
  // the upper bound. If the char ASCII value before the wild-card has the max ASCII value 127, or the first character
  // of the pattern is a wild-card, nullopt is returned.
  // The following table shows examples of the return for some patterns:
  // test%        | %test   | test\x7F% | test            | '' (empty string)
  // {test, tesu} | nullopt | nullopt   | {test, test\0}  | {'', '\0'}
  static std::optional<std::pair<pmr_string, pmr_string>> bounds(const pmr_string& pattern);

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
    MultipleContainsPattern(std::vector<pmr_string> stringssss, bool starts_with_any_char2) : strings(stringssss), starts_with_any_char(starts_with_any_char2) {
      // TODO(Martin): instantiate searchers here
    }
    std::vector<pmr_string> strings;
    bool starts_with_any_char{false};
  };
  struct RE2Pattern final {
    // RE2 cannot be copied or moved. The unique_ptr enables moving the variant in the constructor the LikeMatcher.
    std::unique_ptr<re2::RE2> pattern;
  };

  // Contains one of the specialized patterns from above (StartsWithPattern, ...) or falls back to regex evaluation for
  // remaining pattern.
  using AllPatternVariant =
      std::variant<StartsWithPattern, EndsWithPattern, ContainsPattern, MultipleContainsPattern, RE2Pattern>;

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
    if (std::holds_alternative<StartsWithPattern>(_pattern_variant)) {
      const auto& prefix = std::get<StartsWithPattern>(_pattern_variant).string;
      functor([&](const auto& string) -> bool {
        if (string.size() < prefix.size()) return invert_results;
        return (string.compare(0, prefix.size(), prefix) == 0) ^ invert_results;
      });

    } else if (std::holds_alternative<EndsWithPattern>(_pattern_variant)) {
      const auto& suffix = std::get<EndsWithPattern>(_pattern_variant).string;
      functor([&](const auto& string) -> bool {
        if (string.size() < suffix.size()) return invert_results;
        return (string.compare(string.size() - suffix.size(), suffix.size(), suffix) == 0) ^ invert_results;
      });

    } else if (std::holds_alternative<ContainsPattern>(_pattern_variant)) {
      const auto& contains_str = std::get<ContainsPattern>(_pattern_variant).string;
      // It's really hard to store the searcher in the pattern as it only holds iterators into the string that easily
      // get invalidated when the pattern is passed around.
      const auto searcher = Searcher{contains_str.begin(), contains_str.end()};
      functor([&](const auto& string) -> bool {
        return (std::search(string.begin(), string.end(), searcher) != string.end()) ^ invert_results;
      });

    } else if (std::holds_alternative<MultipleContainsPattern>(_pattern_variant)) {
      const auto& multiple_contains_pattern = std::get<MultipleContainsPattern>(_pattern_variant);
      const auto& contains_strs = multiple_contains_pattern.strings;
      const auto starts_with_any_char = multiple_contains_pattern.starts_with_any_char;

      // TODO(Martin): move to constructor
      std::vector<Searcher> searchers;
      searchers.reserve(contains_strs.size());
      auto string_length_remaining = size_t{0};
      for (const auto& contains_str : contains_strs) {
        searchers.emplace_back(Searcher(contains_str.begin(), contains_str.end()));
        string_length_remaining += contains_str.size();
      }

      functor([&](const auto& string) -> bool {
        auto current_position = string.begin();
        for (auto searcher_idx = size_t{0}; searcher_idx < searchers.size(); ++searcher_idx) {
          const auto current_search_string_length = contains_strs[searcher_idx].size();
          string_length_remaining -= current_search_string_length;
          // End iterator to search up to. If `hello%hello` of `hello%hello%hello` is still remaining, we do not need
          // to search up the end, but can stop earlier. Further ensure, that we don't search_set to be value smaller
          // than current_position.
          auto search_end = std::max(current_position, std::min(string.end(), string.end() - string_length_remaining));
          if (!starts_with_any_char && searcher_idx == 0) {
            // For patterns like 'hello%world', we set the search to check only the first 5 characters.
            search_end = current_position + current_search_string_length;
          }
          
          current_position = std::search(current_position, search_end, searchers[searcher_idx]);
          if (current_position == search_end) { return invert_results; }  // current search term not found
          current_position += current_search_string_length;
        }
        return !invert_results;
      });

    } else if (std::holds_alternative<RE2Pattern>(_pattern_variant)) {
      const auto& pattern = *std::get<RE2Pattern>(_pattern_variant).pattern;
      functor([&](const auto& string) -> bool {
        return re2::RE2::FullMatch(re2::StringPiece{string}, pattern) ^ invert_results;
      });

    } else {
      Fail("Pattern not implemented. Probably a bug.");
    }
  }

 private:
  AllPatternVariant _pattern_variant;
};

std::ostream& operator<<(std::ostream& stream, const LikeMatcher::Wildcard& wildcard);

}  // namespace opossum
