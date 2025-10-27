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
  /**
   * Turn SQL LIKE-pattern into a C++ regex.
   */
  static std::string sql_like_to_regex(pmr_string sql_like);

  static size_t get_index_of_next_wildcard(const pmr_string& pattern, const size_t offset = 0);
  static bool contains_wildcard(const pmr_string& pattern);

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

  template <typename Functor>
  static void resolve_condition(const PredicateCondition predicate_condition, const Functor& functor) {
    if (predicate_condition == PredicateCondition::Like) {
      functor(MatchLike{});
    } else if (predicate_condition == PredicateCondition::NotLike) {
      functor(MatchNotLike{});
    } else if (predicate_condition == PredicateCondition::LikeInsensitive) {
      functor(MatchLikeInsensitive{});
    } else if (predicate_condition == PredicateCondition::NotLikeInsensitive) {
      functor(MatchNotLikeInsensitive{});
    } else {
      Fail("Invalid predicate.");
    }
  }

  /**
   * The functor will be called with a concrete matcher.
   * Usage example:
   *    LikeMatcher::resolve_condition(PredicateCondition::Like [&](const auto& predicate) {
   *      using Predicate = std::decay_t<decltype(predicate)>;
   *      LikeMatcher::resolve_pattern<Predicate>("%hello%", [](const auto& matcher) {
   *        std::cout << matcher("He said hello!");
   *      });
   *    });
   */
  template <typename Predicate, typename Functor>
  static void resolve_pattern(const pmr_string& pattern, const Functor& functor) {
    constexpr auto invert =
        std::is_same_v<Predicate, MatchNotLike> || std::is_same_v<Predicate, MatchNotLikeInsensitive>;
    if constexpr (std::is_same_v<Predicate, MatchLikeInsensitive> ||
                  std::is_same_v<Predicate, MatchNotLikeInsensitive>) {
      resolve_pattern_with_case<invert>(pattern, functor, [](const auto& input, const auto& matching_function) {
        return matching_function(string_to_lower(input));
      });
    } else {
      resolve_pattern_with_case<invert>(pattern, functor, [](const auto& input, const auto& matching_function) {
        return matching_function(input);
      });
    }
  }

  // Used to resolve the like condition once, even if the pattern changes for column vs. column LIKE predicates.
  struct MatchLike {};

  struct MatchNotLike {};

  struct MatchLikeInsensitive {};

  struct MatchNotLikeInsensitive {};

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
  template <typename Functor, typename Casing>
  static void resolve_pattern_type(const pmr_string& pattern, const Casing& resolve_case, const Functor& functor) {
    resolve_case(pattern, [&](const auto& cased_pattern) {
      const auto tokens = pattern_string_to_tokens(cased_pattern);

      if (tokens.size() == 2 && std::holds_alternative<pmr_string>(tokens[0]) &&
          tokens[1] == PatternToken{Wildcard::AnyChars}) {
        // Pattern has the form 'hello%'.
        functor(StartsWithPattern{std::get<pmr_string>(tokens[0])});
        return;
      }

      if (tokens.size() == 2 && tokens[0] == PatternToken{Wildcard::AnyChars} &&
          std::holds_alternative<pmr_string>(tokens[1])) {
        // Pattern has the form '%hello'.
        functor(EndsWithPattern{std::get<pmr_string>(tokens[1])});
        return;
      }

      if (tokens.size() == 3 && tokens[0] == PatternToken{Wildcard::AnyChars} &&
          std::holds_alternative<pmr_string>(tokens[1]) && tokens[2] == PatternToken{Wildcard::AnyChars}) {
        // Pattern has the form '%hello%'.
        functor(ContainsPattern{std::get<pmr_string>(tokens[1])});
        return;
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
        functor(MultipleContainsPattern{strings});
        return;
      }

      return functor(std::regex{sql_like_to_regex(cased_pattern), std::regex::optimize});
    });
  }

  template <bool invert_results, typename Functor, typename Casing>
  static void resolve_pattern_with_case(const pmr_string& pattern, const Functor& functor, const Casing& resolve_case) {
    resolve_pattern_type(pattern, resolve_case, [&](const auto& typed_pattern) {
      using Pattern = std::decay_t<decltype(typed_pattern)>;

      if constexpr (std::is_same_v<Pattern, StartsWithPattern>) {
        const auto& prefix = typed_pattern.string;
        functor([&](const auto& string) -> bool {
          if (string.size() < prefix.size()) {
            return invert_results;
          }

          return resolve_case(string, [&](const auto& cased_string) -> bool {
            return (cased_string.compare(0, prefix.size(), prefix) == 0) ^ invert_results;
          });
        });

      } else if constexpr (std::is_same_v<Pattern, EndsWithPattern>) {
        const auto& suffix = typed_pattern.string;
        functor([&](const auto& string) -> bool {
          if (string.size() < suffix.size()) {
            return invert_results;
          }
          return resolve_case(string, [&](const auto& cased_string) -> bool {
            return (cased_string.compare(cased_string.size() - suffix.size(), suffix.size(), suffix) == 0) ^
                   invert_results;
          });
        });

      } else if constexpr (std::is_same_v<Pattern, ContainsPattern>) {
        const auto& contains_str = typed_pattern.string;
        // It's really hard to store the searcher in the pattern as it only holds iterators into the string that easily
        // get invalidated when the pattern is passed around.
        const auto searcher = Searcher{contains_str.begin(), contains_str.end()};
        functor([&](const auto& string) -> bool {
          return resolve_case(string, [&](const auto& cased_string) -> bool {
            return (std::search(cased_string.begin(), cased_string.end(), searcher) != cased_string.end()) ^
                   invert_results;
          });
        });

      } else if constexpr (std::is_same_v<Pattern, MultipleContainsPattern>) {
        const auto& contains_strs = typed_pattern.strings;
        auto searchers = std::vector<Searcher>{};
        searchers.reserve(contains_strs.size());
        for (const auto& contains_str : contains_strs) {
          searchers.emplace_back(Searcher(contains_str.begin(), contains_str.end()));
        }

        functor([&](const auto& string) -> bool {
          return resolve_case(string, [&](const auto& cased_string) -> bool {
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

      } else if constexpr (std::is_same_v<Pattern, std::regex>) {
        functor([&](const auto& string) -> bool {
          return resolve_case(string, [&](const auto& cased_string) -> bool {
            return std::regex_match(cased_string.cbegin(), cased_string.cend(), typed_pattern) ^ invert_results;
          });
        });

      } else {
        Fail("Pattern not implemented. Probably a bug.");
      }
    });
  }
};

std::ostream& operator<<(std::ostream& stream, const LikeMatcher::Wildcard& wildcard);

}  // namespace hyrise
