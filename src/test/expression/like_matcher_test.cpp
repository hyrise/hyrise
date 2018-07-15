#include <string>

#include "gtest/gtest.h"

#include "expression/evaluation/like_matcher.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

class LikeMatcherTest : public ::testing::Test {
 public:
  bool match(const std::string& value, const std::string& pattern) const {
    auto result = false;
    LikeMatcher{pattern}.resolve(false, [&](const auto& matcher) { result = matcher(value); });
    return result;
  }
};

TEST_F(LikeMatcherTest, PatternToTokens) {
  const auto tokens_a = LikeMatcher::pattern_string_to_tokens("");
  const auto tokens_b = LikeMatcher::pattern_string_to_tokens("%abc%_def__Hello%");

  ASSERT_EQ(tokens_a.size(), 0u);

  ASSERT_EQ(tokens_b.size(), 9u);
  EXPECT_EQ(tokens_b.at(0), LikeMatcher::PatternToken(LikeMatcher::Wildcard::AnyChars));
  EXPECT_EQ(tokens_b.at(1), LikeMatcher::PatternToken("abc"s));
  EXPECT_EQ(tokens_b.at(2), LikeMatcher::PatternToken(LikeMatcher::Wildcard::AnyChars));
  EXPECT_EQ(tokens_b.at(3), LikeMatcher::PatternToken(LikeMatcher::Wildcard::SingleChar));
  EXPECT_EQ(tokens_b.at(4), LikeMatcher::PatternToken("def"s));
  EXPECT_EQ(tokens_b.at(5), LikeMatcher::PatternToken(LikeMatcher::Wildcard::SingleChar));
  EXPECT_EQ(tokens_b.at(6), LikeMatcher::PatternToken(LikeMatcher::Wildcard::SingleChar));
  EXPECT_EQ(tokens_b.at(7), LikeMatcher::PatternToken("Hello"s));
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

}  // namespace opossum
