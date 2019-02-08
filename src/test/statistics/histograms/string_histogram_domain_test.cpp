#include "gtest/gtest.h"

#include "statistics/histograms/string_histogram_domain.hpp"

namespace opossum {

class StringHistogramDomainTest : public ::testing::Test {
 public:
  StringHistogramDomain domain_a{"abcdefghijklmnopqrstuvwxyz", 4u};
};

TEST_F(StringHistogramDomainTest, StringToDomain) {
  StringHistogramDomain domain_a{"abcd", 2u};

  EXPECT_EQ(domain_a.string_to_domain(""), "");
  EXPECT_EQ(domain_a.string_to_domain("a"), "a");
  EXPECT_EQ(domain_a.string_to_domain("aaaaa"), "aaaaa");
  EXPECT_EQ(domain_a.string_to_domain("aaaaz"), "aaaad");
  EXPECT_EQ(domain_a.string_to_domain("abcda"), "abcda");
  EXPECT_EQ(domain_a.string_to_domain("ABCDA"), "aaaaa");
}

TEST_F(StringHistogramDomainTest, NextValue) {
  EXPECT_EQ(domain_a.next_value(""), "a");
  EXPECT_EQ(domain_a.next_value("a"), "aa");
  EXPECT_EQ(domain_a.next_value("ayz"), "ayza");
  EXPECT_EQ(domain_a.next_value("ayzz"), "az");
  EXPECT_EQ(domain_a.next_value("azzz"), "b");
  EXPECT_EQ(domain_a.next_value("z"), "za");
  EXPECT_EQ(domain_a.next_value("df"), "dfa");
  EXPECT_EQ(domain_a.next_value("abcd"), "abce");
  EXPECT_EQ(domain_a.next_value("abaz"), "abb");
  EXPECT_EQ(domain_a.next_value("abzz"), "ac");
  EXPECT_EQ(domain_a.next_value("abca"), "abcb");
  EXPECT_EQ(domain_a.next_value("abaa"), "abab");
  EXPECT_EQ(domain_a.next_value("aaaaa"), "aaab");

  // Special case.
  EXPECT_EQ(domain_a.next_value("zzzz"), "zzzz");

  EXPECT_THROW(domain_a.next_value("A"), std::logic_error);
}

TEST_F(StringHistogramDomainTest, PreviousValue) {
  EXPECT_EQ(domain_a.previous_value(""), "");
  EXPECT_EQ(domain_a.previous_value("a"), "");
  EXPECT_EQ(domain_a.previous_value("ayza"), "ayz");
  EXPECT_EQ(domain_a.previous_value("az"), "ayzz");
  EXPECT_EQ(domain_a.previous_value("b"), "azzz");
  EXPECT_EQ(domain_a.previous_value("za"), "z");
  EXPECT_EQ(domain_a.previous_value("dfa"), "df");
  EXPECT_EQ(domain_a.previous_value("abce"), "abcd");
  EXPECT_EQ(domain_a.previous_value("abb"), "abaz");
  EXPECT_EQ(domain_a.previous_value("ac"), "abzz");
  EXPECT_EQ(domain_a.previous_value("abca"), "abc");
  EXPECT_EQ(domain_a.previous_value("zzzz"), "zzzy");

  EXPECT_THROW(domain_a.previous_value("A"), std::logic_error);
  EXPECT_THROW(domain_a.previous_value("aaaaa"), std::logic_error);
}

TEST_F(StringHistogramDomainTest, StringBefore) {
  StringHistogramDomain domain{"abcdefghijklmnopqrst", 3u};

  EXPECT_EQ(domain.string_before("a", ""), "");
  EXPECT_EQ(domain.string_before("b", ""), "att");
  EXPECT_EQ(domain.string_before("e", ""), "dtt");
  EXPECT_EQ(domain.string_before("bb", "baa"), "bat");
  EXPECT_EQ(domain.string_before("ba", ""), "b");
  EXPECT_EQ(domain.string_before("cba", "cb"), "cb");
  EXPECT_EQ(domain.string_before("bbbb", ""), "bbba");
  EXPECT_EQ(domain.string_before("bbbc", "bbbbc"), "bbbbt");

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

TEST_F(StringHistogramDomainTest, NumberToString) {
  EXPECT_EQ(domain_a.number_to_string(0ul), "");

  // 0 * 26^3 + 1
  EXPECT_EQ(domain_a.number_to_string(1ul), "a");

  // 0 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^2 + 26^1 + 26^0) + 1
  EXPECT_EQ(domain_a.number_to_string(2ul), "aa");

  // 0 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^1 + 26^0) + 1 +
  // 0 * 26^0 + 1
  EXPECT_EQ(domain_a.number_to_string(4ul), "aaaa");

  // 0 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^1 + 26^0) + 1 +
  // 1 * 26^0 + 1
  EXPECT_EQ(domain_a.number_to_string(5ul), "aaab");

  // 0 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 25 * (26^2 + 26^1 + 26^0) + 1 +
  // 25 * (26^1 + 26^0) + 1 +
  // 25 * 26^0 + 1
  EXPECT_EQ(domain_a.number_to_string(18'279ul), "azzz");

  // 1 * (26^3 + 26^2 + 26^1 + 26^0) + 1
  EXPECT_EQ(domain_a.number_to_string(18'280ul), "b");

  // 1 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 0 * (26^2 + 26^1 + 26^0) + 1
  EXPECT_EQ(domain_a.number_to_string(18'281ul), "ba");

  // 1 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 7 * (26^2 + 26^1 + 26^0) + 1 +
  // 9 * (26^1 + 26^0) + 1 +
  // 0 * 26^0 + 1
  EXPECT_EQ(domain_a.number_to_string(23'447ul), "bhja");

  // 2 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 3 * (26^2 + 26^1 + 26^0) + 1 +
  // 4 * (26^1 + 26^0) + 1
  EXPECT_EQ(domain_a.number_to_string(38'778ul), "cde");

  // 25 * (26^3 + 26^2 + 26^1 + 26^0) + 1 +
  // 25 * (26^2 + 26^1 + 26^0) + 1 +
  // 25 * (26^1 + 26^0) + 1 +
  // 25 * 26^0 + 1
  EXPECT_EQ(domain_a.number_to_string(475'254ul), "zzzz");

  EXPECT_THROW(domain_a.number_to_string(475'255ul), std::logic_error);
}

TEST_F(StringHistogramDomainTest, NumberToStringBruteForce) {
  StringHistogramDomain domain{"abcd", 3u};

  EXPECT_EQ(domain.string_to_number("ddd"), 84);
  for (auto number = 0u; number < 84; number++) {
    EXPECT_LT(domain.number_to_string(number), domain.number_to_string(number + 1));
  }
}

TEST_F(StringHistogramDomainTest, StringToNumberBruteForce) {
  StringHistogramDomain domain{"abcd", 3u};

  EXPECT_EQ(domain.string_to_number("ddd"), 84);
  for (auto number = 0u; number < 84; number++) {
    EXPECT_EQ(domain.string_to_number(domain.number_to_string(number)), number);
  }
}

TEST_F(StringHistogramDomainTest, Contains) {
  StringHistogramDomain domain{"abcd", 3u};

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
