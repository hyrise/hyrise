#include "base_test.hpp"
#include "utils/fixed_size_heap.hpp"

namespace hyrise {

namespace {

}  // anonymous namespace

class FixedSizeHeapTest : public BaseTest {};

TEST_F(FixedSizeHeapTest, EqualInputs) {
  constexpr auto N = size_t{16};
  constexpr auto VECTOR_ELEMENTS = size_t{512};

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

  auto result = std::vector<int32_t>{};
  for (auto outer_index = size_t{0}; outer_index < VECTOR_ELEMENTS; ++outer_index) {
    for (auto inner_index = size_t{0}; inner_index < N; ++inner_index) {
      const auto next = heap.next();
      EXPECT_TRUE(next);
      EXPECT_EQ(outer_index, *next);
      result.push_back(*next);
    }
  }
  EXPECT_FALSE(heap.next());
  EXPECT_TRUE(std::ranges::is_sorted(result));
}

TEST_F(FixedSizeHeapTest, PartiallyEmptyLists) {
  constexpr auto N = size_t{10};
  constexpr auto VECTOR_ELEMENTS = size_t{250};

  auto vectors = std::vector<std::vector<int32_t>>(N);
  for (auto vector_index = size_t{2}; vector_index < (N / 2); ++vector_index) {
    for (auto index = size_t{0}; index < VECTOR_ELEMENTS; ++index) {
      vectors[vector_index].push_back(index * vector_index);
    }
  }

  auto begins = std::vector<decltype(vectors[0].cbegin())>(N);
  auto ends = std::vector<decltype(vectors[0].cbegin())>(N);
  for (auto vector_index = size_t{0}; vector_index < N; ++vector_index) {
    begins[vector_index] = vectors[vector_index].cbegin();
    ends[vector_index] = vectors[vector_index].cend();
  }

  auto heap = FixedSizeHeap<int32_t, N, decltype(vectors[0].cbegin())>{begins, ends};

  auto result = std::vector<int32_t>{};
  auto next = heap.next();
  while (next) {
    result.push_back(*next);
    next = heap.next();
  }
  EXPECT_TRUE(std::ranges::is_sorted(result));

  auto result2 = std::vector<int32_t>{};
  for (auto vector_index = size_t{0}; vector_index < N; ++vector_index) {
    result2.insert(result2.end(), vectors[vector_index].cbegin(), vectors[vector_index].cend());
  }
  std::ranges::sort(result2);
  EXPECT_EQ(result, result2);
}

TEST_F(FixedSizeHeapTest, AllEmptyLists) {
  constexpr auto N = size_t{4};

  auto vectors = std::vector<std::vector<int32_t>>(N);

  auto begins = std::vector<decltype(vectors[0].cbegin())>(N);
  auto ends = std::vector<decltype(vectors[0].cbegin())>(N);
  for (auto vector_index = size_t{0}; vector_index < N; ++vector_index) {
    begins[vector_index] = vectors[vector_index].cbegin();
    ends[vector_index] = vectors[vector_index].cend();
  }

  auto heap = FixedSizeHeap<int32_t, N, decltype(vectors[0].cbegin())>{begins, ends};

  EXPECT_FALSE(heap.next());
}

}  // namespace hyrise
