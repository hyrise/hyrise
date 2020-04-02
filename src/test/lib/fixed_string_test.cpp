#include <memory>
#include <string_view>

#include "base_test.hpp"

#include "storage/fixed_string_dictionary_segment/fixed_string.hpp"

namespace opossum {

class FixedStringTest : public BaseTest {
 public:
  void SetUp() override {}
  std::vector<char> char_vector1 = {'f', 'o', 'o'};
  std::vector<char> char_vector2 = {'b', 'a', 'r', 'b', 'a', 'z'};
  FixedString fixed_string1 = FixedString(&char_vector1[0], 3u);
  FixedString fixed_string2 = FixedString(&char_vector2[0], 6u);
};

TEST_F(FixedStringTest, Constructors) {
  std::vector<char> charvector = {'f', 'o', 'o'};
  std::vector<char> charvector2 = {'b', 'a', 'r', 'b', 'a', 'z'};

  auto str1 = FixedString(&charvector[0], 3);
  EXPECT_EQ(str1, "foo");

  auto str2 = FixedString(str1);
  EXPECT_EQ(str2, "foo");

  if (HYRISE_DEBUG) {
    EXPECT_THROW(str1 = FixedString(&charvector2[0], 6u), std::exception);
  } else {
    str1 = FixedString(&charvector2[0], 6u);
    EXPECT_EQ(str1, "bar");
  }
}

TEST_F(FixedStringTest, StringLength) {
  std::vector<char> char_vector = {'f', 'o', 'o', '\0', '\0'};
  FixedString fixed_string = FixedString(&char_vector[0], 5u);

  EXPECT_EQ(fixed_string1.size(), 3u);
  EXPECT_EQ(fixed_string1.maximum_length(), 3u);
  EXPECT_EQ(fixed_string.size(), 3u);
  EXPECT_EQ(fixed_string.maximum_length(), 5u);
  EXPECT_EQ(fixed_string.string(), "foo");
  EXPECT_EQ(fixed_string, "foo");
}

TEST_F(FixedStringTest, CompareFixedStrings) {
  std::vector<char> bar_help = {'b', 'a', 'r', '\0'};
  std::vector<char> bars_help = {'b', 'a', 'r', 's'};
  FixedString bar = FixedString(&bar_help[0], 3u);
  FixedString bar_terminator = FixedString(&bar_help[0], 4u);
  FixedString bars = FixedString(&bars_help[0], 4u);

  EXPECT_TRUE(bar < fixed_string1);
  EXPECT_TRUE(bars < fixed_string1);
  EXPECT_TRUE(bar < bars);
  EXPECT_TRUE(bar_terminator < bars);
  EXPECT_FALSE(bars < bar);
  EXPECT_FALSE(bars < bar_terminator);
  EXPECT_FALSE(bar < bar);

  EXPECT_TRUE(bar == bar);
  EXPECT_TRUE(bar == bar_terminator);
  EXPECT_FALSE(fixed_string2 == bar);
  EXPECT_FALSE(bar == fixed_string2);
}

TEST_F(FixedStringTest, CompareStrings) {
  std::string bar = "bar";
  std::string_view bar_string_view(&bar[0], bar.size());

  EXPECT_FALSE(fixed_string1 < bar);
  EXPECT_TRUE(bar < fixed_string1);

  EXPECT_FALSE(fixed_string1 < bar_string_view);
  EXPECT_TRUE(bar_string_view < fixed_string1);

  EXPECT_FALSE(fixed_string1 < bar.c_str());
  EXPECT_TRUE(bar.c_str() < fixed_string1);

  EXPECT_FALSE(fixed_string1 == bar);
  EXPECT_FALSE(bar == fixed_string1);
  EXPECT_EQ("foo", fixed_string1);
  EXPECT_FALSE(fixed_string1 == bar_string_view);
  EXPECT_FALSE(bar_string_view == fixed_string1);
}

TEST_F(FixedStringTest, Assign) {
  std::vector<char> char_vector3 = {'f', 'o', 'o', 'b', 'a', 'r'};
  FixedString fixed_string3 = FixedString(&char_vector3[0], 6u);
  EXPECT_EQ(fixed_string3, "foobar");

  std::vector<char> char_vector4 = {'b', 'a', 'r'};
  FixedString fixed_string4 = FixedString(&char_vector4[0], 3u);
  EXPECT_EQ(fixed_string4, "bar");

  fixed_string3 = fixed_string4;
  EXPECT_EQ(fixed_string3, "bar");

  fixed_string3 = ((true) ? fixed_string3 : fixed_string4);  // sneak around -Wself-assign-overloaded
  EXPECT_EQ(fixed_string3, "bar");
}

TEST_F(FixedStringTest, Swap) {
  std::vector<char> char_vector = {'b', 'a', 'r'};
  FixedString fixed_string = FixedString(&char_vector[0], 3u);

  std::swap(fixed_string1, fixed_string);
  EXPECT_EQ(fixed_string1, "bar");
  EXPECT_EQ(fixed_string, "foo");
}

TEST_F(FixedStringTest, OutputToStream) {
  std::stringstream sstream;
  sstream << fixed_string1;
  EXPECT_EQ(sstream.str().find("foo"), 0u);
}

}  // namespace opossum
