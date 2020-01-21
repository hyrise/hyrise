#pragma once

#include <random>

#include "types.hpp"

namespace opossum {

/**
 * Get the number of bytes that are allocated on the heap for the given string.
 */
template <typename T>
size_t string_heap_size(const T& string) {
  // Get the default pre-allocated capacity of SSO strings.
  const auto sso_string_capacity = T{}.capacity();

  if (string.capacity() > sso_string_capacity) {
    // For heap-allocated strings, \0 is appended to denote the end of the string. capacity() is used over length()
    // since some libraries (e.g. llvm's libc++) also over-allocate the heap strings
    // (cf. https://shaharmike.com/cpp/std-string/).
    return string.capacity() + 1;
  }

  // valgrind is drunk. This could be `return 0`, but valgrind doesn't like it.
  DebugAssert(string.capacity() - sso_string_capacity == 0, "SSO does not meet expectations");
  return string.capacity() - sso_string_capacity;
}

/**
  * This function iterates over the given string vector @param string_vector strings and sums up the memory usage. Due
  * to the small string optimization (SSO) in most current C++ libraries, each string has an initially allocated buffer
  * (e.g., 15 chars in GCC's libstdc++). If a string is larger, the string is allocated on the heap and the initial
  * string object stores a pointer to the actual string on the heap.
  *
  * Depending on the @param mode, either all strings all considered or a sample is taken and evaluated.
  *
  * Please note, that there are still differences between the stdlib's. Also the full size accumulation is not
  * guaranteed to be 100% accurate for all libraries.
  */
template <typename V>
size_t string_vector_memory_usage(const V& string_vector, const MemoryUsageCalculationMode mode) {
  using StringType = typename V::value_type;
  const auto base_size = sizeof(V);

  // Early out
  if (string_vector.empty()) return base_size;

  constexpr auto sampling_factor = 0.005f;
  constexpr auto min_rows = size_t{10};

  const auto samples_to_draw =
      std::max(min_rows, static_cast<size_t>(std::ceil(sampling_factor * string_vector.size())));

  if (mode == MemoryUsageCalculationMode::Full || samples_to_draw >= string_vector.size()) {
    // Run the (expensive) calculation of aggregating the whole vector's string sizes when full estimation is desired
    // or the given input vector is small.
    auto elements_size = string_vector.capacity() * sizeof(StringType);
    for (const auto& single_string : string_vector) {
      elements_size += string_heap_size(single_string);
    }
    return base_size + elements_size;
  }

  // We manually create sample positions as this turned out to be much faster than using std::iota and std::sample.
  // Since we want an ordered position list (this potentially increases the performance when accessing the segment), we
  // can directly use std::set to generate distinct sample positions. std::set is slightly faster than
  // std::unordered_set + sorting for small sample sizes.
  std::default_random_engine generator{std::random_device{}()};
  std::uniform_int_distribution<size_t> distribution(0ul, samples_to_draw);
  std::set<size_t> sample_set;
  while (sample_set.size() < samples_to_draw) {
    sample_set.insert(distribution(generator));
  }
  // Create vector from set of samples (std::set yields a sorted order)
  std::vector<size_t> sample_positions(sample_set.cbegin(), sample_set.cend());

  // We get the accurate size for all strings in the sample (preallocated buffers + potential heap allocations) and
  // later scale this value using the sampling factor.
  auto elements_size = samples_to_draw * sizeof(StringType);
  for (const auto& sample_position : sample_positions) {
    elements_size += string_heap_size(string_vector[sample_position]);
  }

  const auto actual_sampling_factor = static_cast<float>(samples_to_draw) / string_vector.size();
  return base_size +
         static_cast<size_t>(std::ceil(static_cast<float>(elements_size) / static_cast<float>(actual_sampling_factor)));
}

}  // namespace opossum
