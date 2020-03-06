#include "base_test.hpp"

#include "statistics/statistics_objects/histogram_domain.hpp"

namespace opossum {

class StringHistogramDomainTest : public BaseTest {
 public:
  StringHistogramDomain domain_a{'a', 'z', 4u};
};

TEST_F(StringHistogramDomainTest, StringToDomain) {
  StringHistogramDomain domain_a{'a', 'd', 2u};

  EXPECT_EQ(domain_a.string_to_domain(""), "");
  EXPECT_EQ(domain_a.string_to_domain("a"), "a");
  EXPECT_EQ(domain_a.string_to_domain("aaaaa"), "aaaaa");
  EXPECT_EQ(domain_a.string_to_domain("aaaaz"), "aaaad");
  EXPECT_EQ(domain_a.string_to_domain("abcda"), "abcda");
  EXPECT_EQ(domain_a.string_to_domain("ABCDA"), "aaaaa");
}

TEST_F(StringHistogramDomainTest, NextValue) {
  EXPECT_EQ(domain_a.next_value_clamped(""), "a");
  EXPECT_EQ(domain_a.next_value_clamped("a"), "aa");
  EXPECT_EQ(domain_a.next_value_clamped("ayz"), "ayza");
  EXPECT_EQ(domain_a.next_value_clamped("ayzz"), "az");
  EXPECT_EQ(domain_a.next_value_clamped("azzz"), "b");
  EXPECT_EQ(domain_a.next_value_clamped("z"), "za");
  EXPECT_EQ(domain_a.next_value_clamped("df"), "dfa");
  EXPECT_EQ(domain_a.next_value_clamped("abcd"), "abce");
  EXPECT_EQ(domain_a.next_value_clamped("abaz"), "abb");
  EXPECT_EQ(domain_a.next_value_clamped("abzz"), "ac");
  EXPECT_EQ(domain_a.next_value_clamped("abca"), "abcb");
  EXPECT_EQ(domain_a.next_value_clamped("abaa"), "abab");
  EXPECT_EQ(domain_a.next_value_clamped("aaaaa"), "aaab");

  // Special case.
  EXPECT_EQ(domain_a.next_value_clamped("zzzz"), "zzzz");
}

TEST_F(StringHistogramDomainTest, NextValueThrowsOnInvalidInput) {
  if (!HYRISE_DEBUG) GTEST_SKIP();
  // "A" is not in `domain_a`
  EXPECT_THROW(domain_a.next_value_clamped("A"), std::logic_error);
}

TEST_F(StringHistogramDomainTest, StringToNumber) {
  EXPECT_EQ(domain_a.string_to_number(""), 0ul);

  // 0 * 26^3 + 1
  EXPECT_EQ(domain_a.string_to_number("a"), 1ul);

  // 0 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^2 + 26^1 + 26^0) + 1
  EXPECT_EQ(domain_a.string_to_number("aa"), 2ul);

  // 0 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^1 + 26^0) + 1 +
  // 0 * 26^0 + 1
  EXPECT_EQ(domain_a.string_to_number("aaaa"), 4ul);

  // 0 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^1 + 26^0) + 1 +
  // 1 * 26^0 + 1
  EXPECT_EQ(domain_a.string_to_number("aaab"), 5ul);

  // 0 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 25 * (26^2 + 26^1 + 26^0) + 1 +
  // 25 * (26^1 + 26^0) + 1 +
  // 25 * 26^0 + 1
  EXPECT_EQ(domain_a.string_to_number("azzz"), 18'279ul);

  // 1 * (26^3 + 26^2 + 26^1 + 26^0) + 1
  EXPECT_EQ(domain_a.string_to_number("b"), 18'280ul);

  // 1 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^2 + 26^1 + 26^0) + 1
  EXPECT_EQ(domain_a.string_to_number("ba"), 18'281ul);

  // 1 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 7 * (26^2 + 26^1 + 26^0) + 1 +
  // 9 * (26^1 + 26^0) + 1 +
  // 0 * 26^0 + 1
  EXPECT_EQ(domain_a.string_to_number("bhja"), 23'447ul);

  // 2 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 3 * (26^2 + 26^1 + 26^0) + 1 +
  // 4 * (26^1 + 26^0) + 1
  EXPECT_EQ(domain_a.string_to_number("cde"), 38'778ul);

  // 25 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 25 * (26^2 + 26^1 + 26^0) + 1 +
  // 25 * (26^1 + 26^0) + 1 +
  // 25 * 26^0 + 1
  EXPECT_EQ(domain_a.string_to_number("zzzz"), 475'254ul);

  EXPECT_EQ(domain_a.string_to_number("A"), 1);
  EXPECT_EQ(domain_a.string_to_number("B"), 1);
  EXPECT_EQ(domain_a.string_to_number("aaaaa"), 5);
  EXPECT_EQ(domain_a.string_to_number("aaaaaa"), 5);
  EXPECT_EQ(domain_a.string_to_number("aaaab"), 5);
}

TEST_F(StringHistogramDomainTest, Contains) {
  StringHistogramDomain domain{'a', 'd', 3u};

  EXPECT_TRUE(domain.contains(""));
  EXPECT_TRUE(domain.contains("a"));
  EXPECT_TRUE(domain.contains("d"));
  EXPECT_TRUE(domain.contains("abc"));
  EXPECT_TRUE(domain.contains("ddd"));
  EXPECT_TRUE(domain.contains("abcda"));
  EXPECT_TRUE(domain.contains("abcda"));
  EXPECT_FALSE(domain.contains("e"));
  EXPECT_FALSE(domain.contains("zzzzz"));
}

}  // namespace opossum
