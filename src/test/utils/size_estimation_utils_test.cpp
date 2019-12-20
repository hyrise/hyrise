#include "../base_test.hpp"

#include "types.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

// Check early out of estimation
TEST(SizeEstimationUtilsTest, EmptyVector) {
  const auto empty_vector = pmr_vector<pmr_string>{};
  const auto expected_size = sizeof(pmr_vector<pmr_string>);

  EXPECT_EQ(expected_size, estimate_string_vector_memory_usage(empty_vector, MemoryUsageCalculationMode::Sampled));
  EXPECT_EQ(expected_size, estimate_string_vector_memory_usage(empty_vector, MemoryUsageCalculationMode::Full));
}

// Check correct sampling for shorter vectors than samples being taken
TEST(SizeEstimationUtilsTest, SizeSmallerThanSampleSize) {
  // Small vector with strings that are stored within the initial string object (SSO)
  pmr_vector<pmr_string> small_vector{"a", "b", "c"};

  const auto sso_string_size = sizeof(pmr_string);
  const auto expected_size = sizeof(pmr_vector<pmr_string>) + small_vector.size() * sso_string_size;

  EXPECT_EQ(expected_size, estimate_string_vector_memory_usage(small_vector, MemoryUsageCalculationMode::Sampled));
  EXPECT_EQ(expected_size, estimate_string_vector_memory_usage(small_vector, MemoryUsageCalculationMode::Full));
}

TEST(SizeEstimationUtilsTest, StringVectorExceedingSSOLengths) {
  constexpr auto large_string_length = size_t{500};
  constexpr auto vector_length = size_t{200};
  pmr_vector<pmr_string> string_vector{vector_length, ""};
  string_vector[0] = std::string(large_string_length, '#');
  string_vector[50] = std::string(large_string_length, '#');
  string_vector[100] = std::string(large_string_length, '#');
  string_vector[150] = std::string(large_string_length, '#');

  // For the sampled method, we do not know wheter the SSO-exceeding elements will be in the taken sample. Hence, we
  // only run a sanity check, assuming all values fit within the SSO capacity.
  const auto expected_size_sample = sizeof(pmr_vector<pmr_string>) + vector_length * sizeof(pmr_string);
  EXPECT_LE(expected_size_sample,
            estimate_string_vector_memory_usage(string_vector, MemoryUsageCalculationMode::Sampled));

  // For the full estimation, we should expect an accurate measurement. Four strings should reside in separate heap
  // allocations (+ 1 for the added \0 termination).
  const auto expected_size_full = expected_size_sample + 4 * (large_string_length + 1);
  EXPECT_EQ(expected_size_full, estimate_string_vector_memory_usage(string_vector, MemoryUsageCalculationMode::Full));
  EXPECT_GT(expected_size_full * 1.05,
            estimate_string_vector_memory_usage(string_vector, MemoryUsageCalculationMode::Full));
}

}  // namespace opossum
