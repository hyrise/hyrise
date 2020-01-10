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
template <typename V>
size_t string_vector_memory_usage(const V& string_vector, const MemoryUsageCalculationMode mode) {
  using StringType = typename V::value_type;
  const auto base_size = sizeof(V);

  // Early out
  if (string_vector.empty()) return base_size;

  // Get the default pre-allocated capacity of SSO strings.
  const auto sso_string_capacity = std::string("").capacity();
  auto string_heap_size = [&](const auto& single_string) -> size_t {
    if (single_string.capacity() > sso_string_capacity) {
      // For heap-allocated strings, \0 is appended to denote the end of the string. capacity() is used over length()
      // since some libraries (e.g. llvm's libc++) also over-allocate the heap strings
      // (cf. https://shaharmike.com/cpp/std-string/).
      return single_string.capacity() + 1;
    }
    return 0;
  };

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

  std::default_random_engine generator{std::random_device{}()};
  std::uniform_int_distribution<int> distribution(0, static_cast<int>(samples_to_draw));
  std::set<size_t> sample_set;
  while (sample_set.size() < samples_to_draw) {
    sample_set.insert(static_cast<size_t>(distribution(generator)));
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
