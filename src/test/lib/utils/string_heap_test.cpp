#include "base_test.hpp"
#include "utils/string_heap.hpp"

namespace hyrise {

namespace {

void test_string_heap_strings(StringHeap& string_heap, const auto& input_strings) {
  auto views = std::vector<std::string_view>{};

  for (const auto& input_string : input_strings) {
    views.emplace_back(string_heap.add_string(input_string));
  }

  for (auto index = size_t{0}; index < input_strings.size(); ++index) {
    EXPECT_EQ(views[index], input_strings[index]);
    EXPECT_NE(views[index].data(), input_strings[index].data());
  }
}

}  // anonymous namespace

class StringHeapTest : public BaseTest {};

TEST_F(StringHeapTest, SmallStrings) {
  auto string_heap = StringHeap{};
  const auto test_strings = std::vector<std::string>{"a", "z", "17"};
  test_string_heap_strings(string_heap, test_strings);
  EXPECT_EQ(string_heap.block_count(), 1);
}

TEST_F(StringHeapTest, LargeStrings) {
  auto string_heap = StringHeap{};
  const auto test_strings = std::vector{std::string(100'000, 'a'), std::string(100'000, 'z')};
  test_string_heap_strings(string_heap, test_strings);
  EXPECT_EQ(string_heap.block_count(), 2);
}

// Even though there is much space left in some blocks, the two short strings are in different blocks.
TEST_F(StringHeapTest, SmallAndLargeStringsDifferentBlocks) {
  auto string_heap = StringHeap{};
  auto test_strings = std::vector{std::string{"17"}, std::string(100'000, 'a'), std::string(100'000, 'z'), std::string{"17"}};
  test_string_heap_strings(string_heap, test_strings);
  EXPECT_EQ(string_heap.block_count(), 4);

  string_heap = StringHeap{};
  test_strings = std::vector{std::string{"17"}, std::string(100'000, 'a'), std::string{"17"}, std::string(100'000, 'z')};
  test_string_heap_strings(string_heap, test_strings);
  EXPECT_EQ(string_heap.block_count(), 4);
}

// In case small strings follow each other, they might end up in the same block.
TEST_F(StringHeapTest, SmallAndLargeStringsSharedBlocks) {
  auto string_heap = StringHeap{};
  const auto test_strings = std::vector{std::string(100'000, 'a'), std::string{"17"}, std::string{"17"}, std::string(100'000, 'z')};
  test_string_heap_strings(string_heap, test_strings);
  EXPECT_EQ(string_heap.block_count(), 3);
}

}  // namespace hyrise
