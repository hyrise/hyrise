#pragma once

#include <cmath>

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace opossum {

template <typename T>
class AbstractHistogram;

template <typename T>
std::enable_if_t<std::is_integral_v<T>, T> previous_value(const T value) {
  return value - 1;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, T> previous_value(const T value) {
  return std::nextafter(value, -std::numeric_limits<T>::infinity());
}

template <typename T>
std::enable_if_t<std::is_integral_v<T>, T> next_value(const T value) {
  return value + 1;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, T> next_value(const T value) {
  return std::nextafter(value, std::numeric_limits<T>::infinity());
}

/**
 * Returns the power of base to exp.
 * The standard library function works on doubles and is inaccurate for large numbers.
 */
uint64_t ipow(uint64_t base, uint64_t exp);

namespace histogram {

/**
 * Merge two histograms into one
 */
template <typename T>
std::shared_ptr<AbstractHistogram<T>> merge_histograms(const AbstractHistogram<T>& histogram_a,
                                                       const AbstractHistogram<T>& histogram_b);

/**
 * Reduce the number of bins in a histogram by merging consecutive bins
 * @param max_bin_count     Max number of bins in the resulting histogram
 */
template <typename T>
std::shared_ptr<AbstractHistogram<T>> reduce_histogram(const AbstractHistogram<T>& histogram,
                                                       const size_t max_bin_count);

}  // namespace histogram

}  // namespace opossum

#include "histogram_utils.ipp"