#include <string>
#include <utility>

#include "base_test.hpp"
#include "expression/evaluation/like_matcher.hpp"

namespace hyrise {

class LikeMatcherTest : public BaseTest {
 public:
  bool match(const pmr_string& value, const pmr_string& pattern, const PredicateCondition condition) const {
    auto result = false;

    LikeMatcher::resolve_condition(condition, [&](const auto& predicate) {
      using Predicate = std::decay_t<decltype(predicate)>;
      LikeMatcher::resolve_pattern<Predicate>(pattern, [&](const auto& matcher) {
        result = matcher(value);
      });
    });
    return result;
  }
};

TEST_F(LikeMatcherTest, PatternToTokens) {
  const auto tokens_a = LikeMatcher::pattern_string_to_tokens("");
  const auto tokens_b = LikeMatcher::pattern_string_to_tokens("%abc%_def__Hello%");

  EXPECT_EQ(tokens_a.size(), 0);

  ASSERT_EQ(tokens_b.size(), 9);
  EXPECT_EQ(tokens_b.at(0), LikeMatcher::PatternToken(LikeMatcher::Wildcard::AnyChars));
  EXPECT_EQ(tokens_b.at(1), LikeMatcher::PatternToken(pmr_string{"abc"}));
  EXPECT_EQ(tokens_b.at(2), LikeMatcher::PatternToken(LikeMatcher::Wildcard::AnyChars));
  EXPECT_EQ(tokens_b.at(3), LikeMatcher::PatternToken(LikeMatcher::Wildcard::SingleChar));
  EXPECT_EQ(tokens_b.at(4), LikeMatcher::PatternToken(pmr_string{"def"}));
  EXPECT_EQ(tokens_b.at(5), LikeMatcher::PatternToken(LikeMatcher::Wildcard::SingleChar));
  EXPECT_EQ(tokens_b.at(6), LikeMatcher::PatternToken(LikeMatcher::Wildcard::SingleChar));
  EXPECT_EQ(tokens_b.at(7), LikeMatcher::PatternToken(pmr_string{"Hello"}));
  EXPECT_EQ(tokens_b.at(8), LikeMatcher::PatternToken(LikeMatcher::Wildcard::AnyChars));
}

TEST_F(LikeMatcherTest, Like) {
  EXPECT_TRUE(match("Hello", "Hello", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello", "Hello%", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello", "%Hello%", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello", "%H%%", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello", "H%ello%", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello", "%H%ello%", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello", "H%el%l%o%", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello", "%H%el%l%o%", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello World", "%_%", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello World", "%_World", PredicateCondition::Like));
  EXPECT_TRUE(match("Hello World!! (Nice day)", "H%(%day)", PredicateCondition::Like));
  EXPECT_TRUE(match("Smiley: ^-^", "%^_^%", PredicateCondition::Like));
  EXPECT_TRUE(match("Questionmark: ?", "%_?%", PredicateCondition::Like));

  EXPECT_FALSE(match("hello", "Hello", PredicateCondition::Like));
  EXPECT_FALSE(match("Hello", "Hello_", PredicateCondition::Like));
  EXPECT_FALSE(match("Hello", "He_o", PredicateCondition::Like));
}

TEST_F(LikeMatcherTest, NotLike) {
  EXPECT_FALSE(match("Hello", "Hello", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello", "Hello%", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello", "%Hello%", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello", "%H%%", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello", "H%ello%", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello", "%H%ello%", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello", "H%el%l%o%", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello", "%H%el%l%o%", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello World", "%_%", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello World", "%_World", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Hello World!! (Nice day)", "H%(%day)", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Smiley: ^-^", "%^_^%", PredicateCondition::NotLike));
  EXPECT_FALSE(match("Questionmark: ?", "%_?%", PredicateCondition::NotLike));

  EXPECT_TRUE(match("hello", "Hello", PredicateCondition::NotLike));
  EXPECT_TRUE(match("Hello", "Hello_", PredicateCondition::NotLike));
  EXPECT_TRUE(match("Hello", "He_o", PredicateCondition::NotLike));
}

TEST_F(LikeMatcherTest, LikeInsensitive) {
  EXPECT_TRUE(match("heLlo", "Hello", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo", "Hello%", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo", "%Hello%", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo", "%H%%", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo", "H%ello%", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo", "%H%ello%", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo", "H%el%l%o%", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo", "%H%el%l%o%", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo World", "%_%", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo World", "%_World", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("heLlo World!! (Nice day)", "H%(%day)", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("Smiley: ^-^", "%^_^%", PredicateCondition::LikeInsensitive));
  EXPECT_TRUE(match("Questionmark: ?", "%_?%", PredicateCondition::LikeInsensitive));

  EXPECT_TRUE(match("hello", "Hello", PredicateCondition::LikeInsensitive));
  EXPECT_FALSE(match("Hello", "Hello_", PredicateCondition::LikeInsensitive));
  EXPECT_FALSE(match("Hello", "He_o", PredicateCondition::LikeInsensitive));
}

TEST_F(LikeMatcherTest, NotLikeInsensitive) {
  EXPECT_FALSE(match("heLlo", "Hello", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo", "Hello%", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo", "%Hello%", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo", "%H%%", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo", "H%ello%", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo", "%H%ello%", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo", "H%el%l%o%", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo", "%H%el%l%o%", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo World", "%_%", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo World", "%_World", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("heLlo World!! (Nice day)", "H%(%day)", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("Smiley: ^-^", "%^_^%", PredicateCondition::NotLikeInsensitive));
  EXPECT_FALSE(match("Questionmark: ?", "%_?%", PredicateCondition::NotLikeInsensitive));

  EXPECT_FALSE(match("hello", "Hello", PredicateCondition::NotLikeInsensitive));
  EXPECT_TRUE(match("Hello", "Hello_", PredicateCondition::NotLikeInsensitive));
  EXPECT_TRUE(match("Hello", "He_o", PredicateCondition::NotLikeInsensitive));
}

TEST_F(LikeMatcherTest, LowerUpperBound) {
  const auto pattern = pmr_string("Japan%");
  const auto [lower_bound, upper_bound] = *LikeMatcher::bounds(pattern);
  ASSERT_EQ(lower_bound, "Japan");
  ASSERT_EQ(upper_bound, "Japao");
}

TEST_F(LikeMatcherTest, ASCIIOverflow) {
  // Check that if the char ASCII value before the wildcard has the max ASCII value 127, std::nullopt is returned.
  auto max_ascii_value = pmr_string(1, static_cast<char>(127));
  max_ascii_value.append("%");
  const auto bounds = LikeMatcher::bounds(max_ascii_value);
  EXPECT_FALSE(bounds);
}

TEST_F(LikeMatcherTest, LeadingWildcard) {
  // Check that if the pattern has a leading wildcard, std::nullopt is returned.
  const auto pattern = pmr_string("%Japan");
  const auto bounds = LikeMatcher::bounds(pattern);
  EXPECT_FALSE(bounds);
}

TEST_F(LikeMatcherTest, NoWildcard) {
  const auto pattern = pmr_string("Japan");
  const auto expected_upper_bound = pmr_string("Japan") + '\0';
  const auto [lower_bound, upper_bound] = *LikeMatcher::bounds(pattern);
  ASSERT_EQ(lower_bound, "Japan");
  ASSERT_EQ(upper_bound, expected_upper_bound);
}

TEST_F(LikeMatcherTest, EmptyString) {
  // Check that if the pattern has no wildcard, std::nullopt is returned.
  const auto pattern = pmr_string("");
  const auto expected_upper_bound = pmr_string("") + '\0';
  const auto [lower_bound, upper_bound] = *LikeMatcher::bounds(pattern);
  ASSERT_EQ(lower_bound, "");
  ASSERT_EQ(upper_bound, expected_upper_bound);
}

}  // namespace hyrise
