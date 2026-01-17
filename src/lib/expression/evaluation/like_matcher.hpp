#pragma once

#include <functional>
#include <optional>
#include <ostream>
#include <regex>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/string_utils.hpp"

namespace hyrise {

/**
 * Wraps an SQL LIKE pattern (e.g. "Hello%Wo_ld") which strings can be tested against.
 *
 * Performance optimizations exist for several simple patterns, such as "Hello%" - which is really just a starts_with()
 * check.
 */
class LikeMatcher {
  // A faster search algorithm than the typical byte-wise search if we can reuse the searcher.
  using Searcher = std::boyer_moore_searcher<pmr_string::const_iterator>;

 public:
  explicit LikeMatcher(const pmr_string& pattern, const PredicateCondition predicate_condition);

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
   * The functor will be called with a concrete matcher.
   * Usage example:
   *    LikeMatcher{"%hello%", PredicateCondition::Like}.resolve([](const auto& matcher) {
   *      std::cout << matcher("He said hello!");
   *    }
   */
  template <typename Functor>
  void resolve(const Functor& functor) const {
    if (_predicate_condition == PredicateCondition::Like) {
      _resolve_pattern_with_case<false>(functor, [](const auto& input, const auto& matching_function) {
        return matching_function(input);
      });
      return;
    }

    if (_predicate_condition == PredicateCondition::NotLike) {
      _resolve_pattern_with_case<true>(functor, [](const auto& input, const auto& matching_function) {
        return matching_function(input);
      });
      return;
    }

    if (_predicate_condition == PredicateCondition::LikeInsensitive) {
      _resolve_pattern_with_case<false>(functor, [](const auto& input, const auto& matching_function) {
        return matching_function(string_to_lower(input));
      });
      return;
    }

    if (_predicate_condition == PredicateCondition::NotLikeInsensitive) {
      _resolve_pattern_with_case<true>(functor, [](const auto& input, const auto& matching_function) {
        return matching_function(string_to_lower(input));
      });
      return;
    }

    Fail("Invalid predicate condition.");
  }

  /**
   * To speed up LIKE, there are special implementations available for simple, common patterns. Any other pattern will
   * fall back to regexes.
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
   * Resolves one of the specialised patterns from above (StartsWithPattern, ...) or falls back to a regex for a general
   * pattern.
   */
  using AllPatternVariant =
      std::variant<std::regex, StartsWithPattern, EndsWithPattern, ContainsPattern, MultipleContainsPattern>;

  static AllPatternVariant pattern_string_to_pattern_variant(const pmr_string& pattern, const bool case_insensitive);

 private:
  template <bool invert_results, typename Functor, typename Casing>
  void _resolve_pattern_with_case(const Functor& functor, const Casing& resolve_case) const {
    if (std::holds_alternative<StartsWithPattern>(_pattern_variant)) {
      const auto& prefix = std::get<StartsWithPattern>(_pattern_variant).string;
      functor([&](const auto& string) {
        if (string.size() < prefix.size()) {
          return invert_results;
        }

        return resolve_case(string, [&](const auto& cased_string) {
          return (cased_string.compare(0, prefix.size(), prefix) == 0) != invert_results;
        });
      });
      return;
    }

    if (std::holds_alternative<EndsWithPattern>(_pattern_variant)) {
      const auto& suffix = std::get<EndsWithPattern>(_pattern_variant).string;
      functor([&](const auto& string) {
        if (string.size() < suffix.size()) {
          return invert_results;
        }
        return resolve_case(string, [&](const auto& cased_string) {
          return (cased_string.compare(cased_string.size() - suffix.size(), suffix.size(), suffix) == 0) !=
                 invert_results;
        });
      });
      return;
    }

    if (std::holds_alternative<ContainsPattern>(_pattern_variant)) {
      const auto& contains_str = std::get<ContainsPattern>(_pattern_variant).string;
      // It's really hard to store the searcher in the pattern as it only holds iterators into the string that easily
      // get invalidated when the pattern is passed around.
      const auto searcher = Searcher{contains_str.begin(), contains_str.end()};
      functor([&](const auto& string) {
        return resolve_case(string, [&](const auto& cased_string) {
          return (std::search(cased_string.begin(), cased_string.end(), searcher) != cased_string.end()) !=
                 invert_results;
        });
      });
      return;
    }

    if (std::holds_alternative<MultipleContainsPattern>(_pattern_variant)) {
      const auto& contains_strs = std::get<MultipleContainsPattern>(_pattern_variant).strings;
      auto searchers = std::vector<Searcher>{};
      searchers.reserve(contains_strs.size());
      for (const auto& contains_str : contains_strs) {
        searchers.emplace_back(Searcher(contains_str.begin(), contains_str.end()));
      }

      functor([&](const auto& string) {
        return resolve_case(string, [&](const auto& cased_string) {
          auto current_position = cased_string.begin();
          for (auto searcher_idx = size_t{0}; searcher_idx < searchers.size(); ++searcher_idx) {
            current_position = std::search(current_position, cased_string.end(), searchers[searcher_idx]);
            if (current_position == cased_string.end()) {
              return invert_results;
            }
            current_position += contains_strs[searcher_idx].size();
          }
          return !invert_results;
        });
      });
      return;
    }

    if (std::holds_alternative<std::regex>(_pattern_variant)) {
      const auto& regex = std::get<std::regex>(_pattern_variant);
      functor([&](const auto& string) {
        return resolve_case(string, [&](const auto& cased_string) {
          return std::regex_match(cased_string.cbegin(), cased_string.cend(), regex) != invert_results;
        });
      });

      return;
    }

    Fail("Pattern not implemented. Probably a bug.");
  }

  PredicateCondition _predicate_condition;
  AllPatternVariant _pattern_variant;
};

std::ostream& operator<<(std::ostream& stream, const LikeMatcher::Wildcard& wildcard);

}  // namespace hyrise
