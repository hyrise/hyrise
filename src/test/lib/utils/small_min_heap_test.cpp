#include "base_test.hpp"

#include "utils/small_min_heap.hpp"

namespace hyrise {

class SmallMinHeapTest : public BaseTest {};

TEST_F(SmallMinHeapTest, Constructor) {
  const auto heap = SmallMinHeap<8, int>(std::array{4, 1});
  EXPECT_EQ(heap.size(), 2);
  EXPECT_EQ(heap.top(), 1);
}

TEST_F(SmallMinHeapTest, Pop) {
  auto heap = SmallMinHeap<8, int>(std::array{4, 2});
  EXPECT_EQ(heap.top(), 2);
  EXPECT_EQ(heap.pop(), 2);
  EXPECT_EQ(heap.size(), 1);
  EXPECT_EQ(heap.top(), 4);
  EXPECT_EQ(heap.pop(), 4);
  EXPECT_EQ(heap.size(), 0);
}

TEST_F(SmallMinHeapTest, Push) {
  auto heap = SmallMinHeap<8, int>(std::array{4, 2});
  heap.push(3);
  EXPECT_EQ(heap.size(), 3);
  EXPECT_EQ(heap.top(), 2);
  heap.push(1);
  EXPECT_EQ(heap.size(), 4);
  EXPECT_EQ(heap.top(), 1);
  EXPECT_EQ(heap.pop(), 1);
  EXPECT_EQ(heap.pop(), 2);
  EXPECT_EQ(heap.pop(), 3);
  EXPECT_EQ(heap.pop(), 4);
  EXPECT_EQ(heap.size(), 0);
}

}  // namespace hyrise
