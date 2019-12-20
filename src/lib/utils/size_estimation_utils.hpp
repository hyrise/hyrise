#pragma once

#include <random>

#include "types.hpp"

namespace opossum {

// In case the initially preallocated capacity is too small, the string is allocated on the heap.
template <typename T>
size_t memory_usage_of_string_objects(const T& string_vector) {
  // https://stackoverflow.com/questions/2037209/what-is-a-null-terminated-string
  const auto sso_string_capacity = std::string("").capacity();

  // Add up sizes for vector object and the pre-initialized strings.
  auto bytes = string_vector.capacity() * sizeof(pmr_string);

  for (const auto& v : string_vector) {
    if (v.length() > sso_string_capacity) {
      // Length is guaranteed to return the number of bytes, independent from any potential encoding.
      // For each string, \0 is appended to denote the end of the string.
      bytes += v.length() + 1;
    }
  }

  return bytes;
}

template <typename T>
size_t estimate_string_vector_memory_usage(const T& string_vector, const MemoryUsageCalculationMode mode) {
  // std::cout << "BBBBBBB" << std::endl;
  const auto base_size = sizeof(T);

  if (string_vector.empty()) return base_size;

  if (mode == MemoryUsageCalculationMode::Full) {
    // Simple (but expensive case): the whole vector's string sizes are aggregated.
    // std::cout << "estimating fully" << std::endl;
    return base_size + memory_usage_of_string_objects(string_vector);
  }

  // For the sampled estimation case, we first create a vector of sample string values and estimate that vector fully.
  constexpr auto sampling_factor = 0.01f;
  constexpr auto min_rows = size_t{10};

  auto samples_to_draw = std::max(min_rows, static_cast<size_t>(std::ceil(sampling_factor * string_vector.size())));
  samples_to_draw = std::min(samples_to_draw, string_vector.size());
  const auto actual_sampling_factor = static_cast<float>(samples_to_draw) / string_vector.size();

  pmr_vector<pmr_string> samples;
  samples.reserve(samples_to_draw);

  std::sample(string_vector.begin(), string_vector.end(), std::back_inserter(samples), samples_to_draw,
              std::random_device{});
  const auto sample_vector_memory_usage = memory_usage_of_string_objects(samples);

  // std::cout << "estimating " << samples_to_draw << " samples of " << string_vector.size() << std::endl;

  return base_size + static_cast<size_t>(std::ceil(sample_vector_memory_usage / actual_sampling_factor));
}

}  // namespace opossum
