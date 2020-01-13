#include "../base_test.hpp"

#include "types.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

class SizeEstimationUtilsTest : public BaseTest {};

TEST_F(SizeEstimationUtilsTest, SingleString) {
  auto string1 = std::string{"a"};
  auto string2 = pmr_string{"b"};

  EXPECT_EQ(string_heap_size(string1), 0);
  EXPECT_EQ(string_heap_size(string2), 0);
}

// Check early out of estimation
TEST_F(SizeEstimationUtilsTest, EmptyVector) {
  const auto empty_vector = pmr_vector<pmr_string>{};
  const auto expected_size = sizeof(pmr_vector<pmr_string>);

  EXPECT_EQ(string_vector_memory_usage(empty_vector, MemoryUsageCalculationMode::Sampled), expected_size);
  EXPECT_EQ(string_vector_memory_usage(empty_vector, MemoryUsageCalculationMode::Full), expected_size);
}

// Check that sampling works as expected when the input vector is shorter than the minimal sample size.
TEST_F(SizeEstimationUtilsTest, SizeSmallerThanSampleSize) {
  // Small vector with strings that are stored within the initial string object (SSO)
  pmr_vector<pmr_string> small_vector{"a", "b", "c"};

  const auto sso_string_size = sizeof(pmr_string);
  const auto expected_size = sizeof(pmr_vector<pmr_string>) + small_vector.size() * sso_string_size;

  EXPECT_EQ(string_vector_memory_usage(small_vector, MemoryUsageCalculationMode::Sampled), expected_size);
  EXPECT_EQ(string_vector_memory_usage(small_vector, MemoryUsageCalculationMode::Full), expected_size);
}

TEST_F(SizeEstimationUtilsTest, StringVectorExceedingSSOLengths) {
  constexpr auto large_string_length = size_t{500};
  constexpr auto vector_length = size_t{200};
  const auto large_string = pmr_string(large_string_length, '#');

  pmr_vector<pmr_string> string_vector{vector_length, ""};
  string_vector[0] = large_string;
  string_vector[50] = large_string;
  string_vector[100] = large_string;
  string_vector[150] = large_string;

  // For the sampled method, we do not know whether the SSO-exceeding elements will be in the taken sample. Hence, we
  // only run a sanity check, assuming all values fit within the SSO capacity.
  const auto expected_size_sample = sizeof(pmr_vector<pmr_string>) + vector_length * sizeof(pmr_string);
  EXPECT_GE(string_vector_memory_usage(string_vector, MemoryUsageCalculationMode::Sampled), expected_size_sample);

  // For the full estimation, we can expect a rather accurate measurement. Four strings should reside on the heap.
  const auto expected_size_full = expected_size_sample + 4 * (large_string.capacity() + 1);
  EXPECT_EQ(string_vector_memory_usage(string_vector, MemoryUsageCalculationMode::Full), expected_size_full);
}

}  // namespace opossum
