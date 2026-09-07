#include <exception>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "storage/fixed_string_dictionary_segment/fixed_string.hpp"

namespace hyrise {

class FixedStringTest : public BaseTest {
 public:
  void SetUp() override {}

  std::vector<char> char_vector1 = {'f', 'o', 'o'};
  std::vector<char> char_vector2 = {'b', 'a', 'r', 'b', 'a', 'z'};
  FixedString fixed_string1 = FixedString(&char_vector1[0], 3);
  FixedString fixed_string2 = FixedString(&char_vector2[0], 6);
};

TEST_F(FixedStringTest, Constructors) {
  auto charvector = std::vector<char>{'f', 'o', 'o'};
  auto charvector2 = std::vector<char>{'b', 'a', 'r', 'b', 'a', 'z'};

  auto str1 = FixedString(&charvector[0], 3);
  EXPECT_EQ(str1, "foo");

  auto str2 = FixedString(str1);
  EXPECT_EQ(str2, "foo");

  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(str1 = FixedString(&charvector2[0], 6), std::exception);
  } else {
    str1 = FixedString(&charvector2[0], 6);
    EXPECT_EQ(str1, "bar");
  }
}

TEST_F(FixedStringTest, StringLength) {
  auto char_vector = std::vector<char>{'f', 'o', 'o', '\0', '\0'};
  auto fixed_string = FixedString(&char_vector[0], 5);

  EXPECT_EQ(fixed_string1.size(), 3);
  EXPECT_EQ(fixed_string1.maximum_length(), 3);
  EXPECT_EQ(fixed_string.size(), 3);
  EXPECT_EQ(fixed_string.maximum_length(), 5);
  EXPECT_EQ(fixed_string.string(), "foo");
  EXPECT_EQ(fixed_string, "foo");
}

TEST_F(FixedStringTest, CompareFixedStrings) {
  auto bar_help = std::vector<char>{'b', 'a', 'r', '\0'};
  auto bars_help = std::vector<char>{'b', 'a', 'r', 's'};
  auto bar = FixedString(&bar_help[0], 3);
  auto bar_terminator = FixedString(&bar_help[0], 4);
  auto bars = FixedString(&bars_help[0], 4);

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
  auto bar = std::string{"bar"};
  auto bar_string_view = std::string_view(&bar[0], bar.size());

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
  auto char_vector3 = std::vector<char>{'f', 'o', 'o', 'b', 'a', 'r'};
  auto fixed_string3 = FixedString(&char_vector3[0], 6);
  EXPECT_EQ(fixed_string3, "foobar");

  auto char_vector4 = std::vector<char>{'b', 'a', 'r'};
  auto fixed_string4 = FixedString(&char_vector4[0], 3);
  EXPECT_EQ(fixed_string4, "bar");

  fixed_string3 = fixed_string4;
  EXPECT_EQ(fixed_string3, "bar");

  fixed_string3 = ((true) ? fixed_string3 : fixed_string4);  // sneak around -Wself-assign-overloaded
  EXPECT_EQ(fixed_string3, "bar");
}

TEST_F(FixedStringTest, Swap) {
  auto char_vector = std::vector<char>{'b', 'a', 'r'};
  auto fixed_string = FixedString(&char_vector[0], 3);

  std::swap(fixed_string1, fixed_string);
  EXPECT_EQ(fixed_string1, "bar");
  EXPECT_EQ(fixed_string, "foo");
}

TEST_F(FixedStringTest, OutputToStream) {
  auto sstream = std::stringstream{};
  sstream << fixed_string1;
  EXPECT_EQ(sstream.str().find("foo"), 0);
}

TEST_F(FixedStringTest, MoveWithOwnsMemory) {
  auto fixed_string = FixedString(fixed_string1);
  auto new_fixed_string = FixedString(fixed_string2);
  new_fixed_string = std::move(fixed_string);

  EXPECT_EQ(new_fixed_string, fixed_string1);
  // The maximum_length being set correctly implies that the move operator was successful.
  EXPECT_EQ(new_fixed_string.maximum_length(), fixed_string1.maximum_length());
}

TEST_F(FixedStringTest, SwapFixedString) {
  auto char_vector = std::vector<char>{'b', 'a', 'r'};
  auto fixed_string = FixedString(&char_vector[0], 3);

  fixed_string.swap(fixed_string1);
  EXPECT_EQ(fixed_string1.string(), "bar");
  EXPECT_EQ(fixed_string.string(), "foo");

  swap(fixed_string, fixed_string1);
  EXPECT_EQ(fixed_string1.string(), "bar");
  EXPECT_EQ(fixed_string.string(), "foo");
}

}  // namespace hyrise
