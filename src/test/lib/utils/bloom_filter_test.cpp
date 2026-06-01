#include <gtest/gtest.h>

#include <bitset>
#include <cstddef>
#include <random>

#include "base_test.hpp"
#include "utils/bloom_filter.hpp"

namespace hyrise {

class BloomFilterTest : public BaseTest {};

TEST_F(BloomFilterTest, DefaultBloomFilter) {
  auto bloom_filter = BloomFilter{};

  auto values = std::vector<size_t>{};
  for (auto value = size_t{0}; value < 1024; ++value) {
    values.push_back(value);
  }
  for (auto value = size_t{1'000'000}; value < 100'000'000; value *= 2) {
    values.push_back(value);
  }
  for (auto value = size_t{std::numeric_limits<uint64_t>::max()}; value > std::numeric_limits<uint64_t>::max() / 32;
       value /= 2) {
    values.push_back(value);
  }

  for (auto value : values) {
    EXPECT_TRUE(bloom_filter.does_not_contain(value));
  }

  for (auto value : values) {
    bloom_filter.add(value);
  }

  for (auto value : values) {
    EXPECT_FALSE(bloom_filter.does_not_contain(value));
  }
}

// TEST_F(BloomFilterTest, RandomBloomFilter) {
//   auto random_device = std::random_device{};
//   auto generator = std::mt19937_64{random_device()};
//   auto distribution = std::uniform_int_distribution<size_t>{};

//   for (auto run = size_t{0}; run < 100; ++run) {
//     auto bloom_filter = BloomFilter{};
//     for (auto index = size_t{0}; index < 10'000; ++index) {
//         const auto random_value = distribution(generator);
//         ASSERT_TRUE(bloom_filter.does_not_contain(random_value));
//     }
//     std::cerr << ".";
//   }

//   for (auto run = size_t{0}; run < 1'000; ++run) {
//     auto bloom_filter = BloomFilter{};
//     for (auto index = size_t{0}; index < 10'000; ++index) {
//         const auto random_value = distribution(generator);
//         bloom_filter.add(random_value);
//         ASSERT_FALSE(bloom_filter.does_not_contain(random_value));
//     }
//     std::cerr << ".";
//   }
// }

// TEST_F(BloomFilterTest, AllTrueBloomFilter) {
//   auto bloom_filter = BloomFilter{true};
//   const auto* view = bloom_filter.non_atomic_view();

//   for (auto index = size_t{0}; index < bloom_filter.size(); ++index) {
//     ASSERT_EQ(view[index], std::numeric_limits<uint64_t>::max());
//   }
//   for (auto value = size_t{0}; value < std::numeric_limits<uint32_t>::max(); ++value) {
//     EXPECT_FALSE(bloom_filter.does_not_contain(value));
//   }
//   EXPECT_FALSE(bloom_filter.does_not_contain(std::numeric_limits<uint64_t>::max()));
// }

}  // namespace hyrise
