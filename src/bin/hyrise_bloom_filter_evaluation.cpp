#include <iostream>
#include <random>
#include <string>
#include <vector>
// #include <cstdint>

// #include "types.hpp"
#include "utils/bloom_filter.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

std::vector<int32_t> generate_data(size_t vector_size, double distinctiveness) {
  std::cout << "Generating vector of size: " << vector_size << " and distinctiveness: " << distinctiveness << "\n";
  // Calculate the range based on distinctiveness
  // For distinctiveness = 1.0, use full int32_t range
  // For distinctiveness = 0.1, use range that gives ~10% distinct values
  size_t range = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) -
                 static_cast<int64_t>(std::numeric_limits<int32_t>::min()) + 1;

  // Calculate effective range: smaller distinctiveness = smaller range
  // For distinctiveness = 1.0, use the full range
  // For smaller distinctiveness, calculate range to get desired distinct ratio
  if (distinctiveness < 1.0) {
    range = static_cast<size_t>(static_cast<double>(vector_size) * distinctiveness);
  }

  // Calculate the actual range bounds
  auto range_min = std::numeric_limits<int32_t>::min();
  auto range_max = static_cast<int32_t>(static_cast<size_t>(range_min) + range - 1);

  // Set up random number generation
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int32_t> dis(range_min, range_max);

  // Generate the vector
  std::vector<int32_t> result;
  result.reserve(vector_size);

  for (size_t i = 0; i < vector_size; ++i) {
    result.emplace_back(dis(gen));
  }

  std::cout << "Finished vector generation.\n";
  return result;
}

template <uint8_t FilterSize, uint8_t K>
void run_bloom_filter_evaluation(const std::vector<int32_t>& vec, const uint8_t hash_function) {
  std::cout << "Evaluation Bloom filter with size: " << std::to_string(FilterSize) << ", K: " << std::to_string(K)
            << ", and Hash: " << std::to_string(hash_function) << "\n";
  auto bloom_filter = BloomFilter<FilterSize, K>{};
  bloom_filter.insert(1);
}

std::vector<size_t> vector_sizes = {10000, 1000000, 100000000};
std::vector<double> distinctivenesses = {0.01, 0.1, 1};
uint8_t hash_functions = 1;

int main() {
  for (const auto vector_size : vector_sizes) {
    for (const auto distinctiveness : distinctivenesses) {
      const auto vec = generate_data(vector_size, distinctiveness);

      for (uint8_t hash_function = 0; hash_function < hash_functions; ++hash_function) {
        run_bloom_filter_evaluation<16, 1>(vec, hash_function);
        run_bloom_filter_evaluation<17, 1>(vec, hash_function);
        run_bloom_filter_evaluation<18, 1>(vec, hash_function);
        run_bloom_filter_evaluation<19, 1>(vec, hash_function);
        run_bloom_filter_evaluation<20, 1>(vec, hash_function);
        run_bloom_filter_evaluation<21, 1>(vec, hash_function);
        run_bloom_filter_evaluation<16, 2>(vec, hash_function);
        run_bloom_filter_evaluation<17, 2>(vec, hash_function);
        run_bloom_filter_evaluation<18, 2>(vec, hash_function);
        run_bloom_filter_evaluation<19, 2>(vec, hash_function);
        run_bloom_filter_evaluation<20, 2>(vec, hash_function);
        run_bloom_filter_evaluation<21, 2>(vec, hash_function);
        run_bloom_filter_evaluation<16, 3>(vec, hash_function);
        run_bloom_filter_evaluation<17, 3>(vec, hash_function);
        run_bloom_filter_evaluation<18, 3>(vec, hash_function);
        run_bloom_filter_evaluation<19, 3>(vec, hash_function);
        run_bloom_filter_evaluation<20, 3>(vec, hash_function);
        run_bloom_filter_evaluation<21, 3>(vec, hash_function);
      }
    }
  }

  return 0;
}
