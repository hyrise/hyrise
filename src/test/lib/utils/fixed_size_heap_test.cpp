#include "base_test.hpp"
#include "utils/fixed_size_heap.hpp"

namespace hyrise {

namespace {

}  // anonymous namespace

class FixedSizeHeapTest : public BaseTest {};

TEST_F(FixedSizeHeapTest, HeapTest) {
  constexpr auto N = size_t{16};
  constexpr auto VECTOR_ELEMENTS = size_t{1024};

  auto vectors = std::vector<std::vector<int32_t>>(N);
  for (auto& vector : vectors) {
    for (auto index = size_t{0}; index < VECTOR_ELEMENTS; ++index) {
      vector.push_back(index);
    }
  }

  auto begins = std::vector<decltype(vectors[0].cbegin())>(N);
  auto ends = std::vector<decltype(vectors[0].cbegin())>(N);
  for (auto vector_index = size_t{0}; vector_index < N; ++vector_index) {
    begins[vector_index] = vectors[vector_index].cbegin();
    ends[vector_index] = vectors[vector_index].cend();
  }

  auto heap = FixedSizeHeap<int32_t, N, decltype(vectors[0].cbegin())>{begins, ends};

  for (auto outer_index = size_t{0}; outer_index < VECTOR_ELEMENTS; ++outer_index) {
    for (auto inner_index = size_t{0}; inner_index < N; ++inner_index) {
      const auto next = heap.next();
      EXPECT_TRUE(next);
      EXPECT_EQ(outer_index, *next);
    }
  }
  EXPECT_FALSE(heap.next());
}

}  // namespace hyrise
