#pragma once

#include <random>

#include "types.hpp"

namespace opossum {

/**
  * This function iterates over the given string vector @param string_vector strings and sums up the memory usage. Due
  * to the small string optimization (SSO) in most current C++ libraries, each string has an initially allocated buffer
  * (e.g., 15 chars in GCC's libstdc++). If a string is larger, the string is allocated on the heap and the initial
  * string object stores a pointer to the actual string on the heap.
  *
  * Depending on the @param mode, either all strings all considered or a sample is taken and evaluated.
  *
  * Please note, that there are still differences between the stdlib's. Thus, even the full size accumulation is not
  * guaranteed to be 100% accurate for all libraries.
  */
template <typename T>
size_t string_vector_memory_usage(const T& string_vector, const MemoryUsageCalculationMode mode) {
  const auto base_size = sizeof(T);

  // Early out
  if (string_vector.empty()) return base_size;

  // Get the default pre-allocated capacity of SSO strings.
  const auto sso_string_capacity = std::string("").capacity();
  auto sso_exceeding_bytes = [&](const auto& single_string) -> size_t {
    if (single_string.capacity() > sso_string_capacity) {
      // For heap-allocated strings, \0 is appended to denote the end of the string. capacity() is used over length()
      // since some libraries (e.g. llvm's libc++) also over-allocate the heap strings
      // (cf. https://shaharmike.com/cpp/std-string/).
      return single_string.capacity() + 1;
    }
    return 0;
  };

  constexpr auto sampling_factor = 0.01f;
  constexpr auto min_rows = size_t{10};

  auto samples_to_draw = std::max(min_rows, static_cast<size_t>(std::ceil(sampling_factor * string_vector.size())));
  samples_to_draw = std::min(samples_to_draw, string_vector.size());

  if (mode == MemoryUsageCalculationMode::Full || samples_to_draw >= string_vector.size()) {
    // Run the (expensive) calculation of aggregating the whole vector's string sizes when full estimation is desired
    // or the given input vector is small.
    auto elements_size = string_vector.capacity() * sizeof(T);
    for (const auto& single_string : string_vector) {
      elements_size += sso_exceeding_bytes(single_string);
    }
    return base_size + elements_size;
  }

  // Create a vector of all possible positions and later use sample to collect a sample of positions. We do not
  // randomly get positions, since we would need use a set to ensure that we do not have duplicates. The current
  // approach works mostly sequentially and returns a unique and sorted (better performance when later accessing
  // the samples positions) vector of sample positions.
  std::vector<size_t> all_positions(string_vector.size());
  std::iota(all_positions.begin(), all_positions.end(), 0);

  std::vector<size_t> sample_positions(samples_to_draw);
  std::sample(all_positions.begin(), all_positions.end(), sample_positions.begin(), samples_to_draw,
              std::mt19937{std::random_device{}()});

  // We get the accurate size for all strings in the sample (preallocated buffers + potential heap allocations) and
  // then scale this value using the sampling factor.
  auto elements_size = samples_to_draw * sizeof(T);
  for (const auto& sample_position : sample_positions) {
    elements_size += sso_exceeding_bytes(string_vector[sample_position]);
  }

  const auto actual_sampling_factor = static_cast<float>(samples_to_draw) / string_vector.size();
  return base_size +
         static_cast<size_t>(std::ceil(static_cast<float>(elements_size) / static_cast<float>(actual_sampling_factor)));
}

}  // namespace opossum
