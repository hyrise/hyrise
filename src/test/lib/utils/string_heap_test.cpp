#include "base_test.hpp"
#include "utils/string_heap.hpp"
#include "utils/small_prefix_string_view.hpp"

namespace hyrise {

namespace {

void test_string_heap_strings(StringHeap& string_heap, const auto& input_strings) {
  auto views = std::vector<SmallPrefixStringView>{};

  for (const auto& input_string : input_strings) {
    views.emplace_back(string_heap.add_string(input_string));
  }

  for (auto index = size_t{0}; index < input_strings.size(); ++index) {
    EXPECT_EQ(views[index].get_string_view(), input_strings[index]);
    EXPECT_NE(views[index].data(), input_strings[index].data());
  }
}

}  // anonymous namespace

class StringHeapTest : public BaseTest {};

TEST_F(StringHeapTest, SmallInlinedStrings) {
  auto string_heap = StringHeap{};
  const auto test_strings = std::vector<std::string>{"a", "z", "17"};
  test_string_heap_strings(string_heap, test_strings);

  // Short strings are stored in-place.
  EXPECT_EQ(string_heap.block_count(), 0);
}

TEST_F(StringHeapTest, LargeStrings) {
  auto string_heap = StringHeap{};
  const auto test_strings = std::vector{std::string(StringHeap::BLOCK_SIZE + 17, 'a'), std::string(StringHeap::BLOCK_SIZE + 17, 'z')};
  test_string_heap_strings(string_heap, test_strings);
  EXPECT_EQ(string_heap.block_count(), 2);
}

// Even though there is much space left in some blocks, the two short (but not inlined) strings are in different blocks.
TEST_F(StringHeapTest, SmallAndLargeStringsDifferentBlocks) {
  auto string_heap = StringHeap{};
  auto test_strings = std::vector{std::string(SmallPrefixStringView::MAX_DIRECT_SIZE + 1, 'a'), std::string(StringHeap::BLOCK_SIZE + 17, 'b'), std::string(StringHeap::BLOCK_SIZE + 17, 'c'), std::string(SmallPrefixStringView::MAX_DIRECT_SIZE + 1, 'd')};
  test_string_heap_strings(string_heap, test_strings);
  EXPECT_EQ(string_heap.block_count(), 4);

  string_heap = StringHeap{};
  test_strings = std::vector{std::string(SmallPrefixStringView::MAX_DIRECT_SIZE + 1, 'a'), std::string(StringHeap::BLOCK_SIZE + 17, 'b'), std::string(SmallPrefixStringView::MAX_DIRECT_SIZE + 1, 'c'), std::string(StringHeap::BLOCK_SIZE + 17, 'd')};
  test_string_heap_strings(string_heap, test_strings);
  EXPECT_EQ(string_heap.block_count(), 4);
}

// In case small strings follow each other, they might end up in the same block.
TEST_F(StringHeapTest, SmallAndLargeStringsSharedBlocks) {
  auto string_heap = StringHeap{};
  const auto test_strings = std::vector{std::string(StringHeap::BLOCK_SIZE + 17, 'a'),
                                        std::string(SmallPrefixStringView::MAX_DIRECT_SIZE + 2, 'b'),
                                        std::string(SmallPrefixStringView::MAX_DIRECT_SIZE + 2, 'c'),
                                        std::string(StringHeap::BLOCK_SIZE + 17, 'd')};
  test_string_heap_strings(string_heap, test_strings);
  EXPECT_EQ(string_heap.block_count(), 3);
}

}  // namespace hyrise
