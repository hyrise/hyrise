#include <string>
#include <utility>

#include "base_test.hpp"

#include "expression/evaluation/like_matcher.hpp"

using namespace std::string_literals;  // NOLINT

namespace hyrise {

class LikeMatcherTest : public BaseTest {
 public:
  bool match(const std::string& value, const std::string& pattern) const {
    auto result = false;
    LikeMatcher{pmr_string{pattern}}.resolve(false, [&](const auto& matcher) { result = matcher(pmr_string{value}); });
    return result;
  }
};

TEST_F(LikeMatcherTest, PatternToTokens) {
  const auto tokens_a = LikeMatcher::pattern_string_to_tokens("");
  const auto tokens_b = LikeMatcher::pattern_string_to_tokens("%abc%_def__Hello%");

  ASSERT_EQ(tokens_a.size(), 0u);

  ASSERT_EQ(tokens_b.size(), 9u);
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

TEST_F(LikeMatcherTest, Matching) {
  EXPECT_TRUE(match("Hello", "Hello"));
  EXPECT_TRUE(match("Hello", "Hello%"));
  EXPECT_TRUE(match("Hello", "%H%%"));
  EXPECT_TRUE(match("Hello", "%H%ello%"));
  EXPECT_TRUE(match("Hello", "%H%ello%"));
  EXPECT_TRUE(match("Hello World", "%_%"));
  EXPECT_TRUE(match("Hello World", "%_World"));
  EXPECT_TRUE(match("Hello World!! (Nice day)", "H%(%day)"));
  EXPECT_TRUE(match("Smiley: ^-^", "%^_^%"));
  EXPECT_TRUE(match("Questionmark: ?", "%_?%"));
}

TEST_F(LikeMatcherTest, NotMatching) {
  EXPECT_FALSE(match("hello", "Hello"));
  EXPECT_FALSE(match("Hello", "Hello_"));
  EXPECT_FALSE(match("Hello", "He_o"));
}

TEST_F(LikeMatcherTest, LowerUpperBound) {
  const auto pattern = pmr_string("Japan%");
  const auto bounds = LikeMatcher::bounds(pattern);
  const auto [lower_bound, upper_bound] = bounds.value();
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
  const auto bounds = LikeMatcher::bounds(pattern);
  const auto [lower_bound, upper_bound] = bounds.value();
  ASSERT_EQ(lower_bound, "Japan");
  ASSERT_EQ(upper_bound, expected_upper_bound);
}

TEST_F(LikeMatcherTest, EmptyString) {
  // Check that if the pattern has no wildcard, std::nullopt is returned.
  const auto pattern = pmr_string("");
  const auto expected_upper_bound = pmr_string("") + '\0';
  const auto bounds = LikeMatcher::bounds(pattern);
  const auto [lower_bound, upper_bound] = bounds.value();
  ASSERT_EQ(lower_bound, "");
  ASSERT_EQ(upper_bound, expected_upper_bound);
}

}  // namespace hyrise
