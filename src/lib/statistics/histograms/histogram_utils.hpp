#pragma once

#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "string_histogram_domain.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;
class BaseSegment;
template <typename T>
class GenericHistogram;
class Table;

using HistogramCountType = float;

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
 * Reduce the number of bins in a histogram by merging consecutive bins
 * @param max_bin_count     Max number of bins in the resulting histogram
 */
template <typename T>
std::shared_ptr<GenericHistogram<T>> reduce_histogram(const AbstractHistogram<T>& histogram,
                                                      const size_t max_bin_count);

template <typename T>
std::vector<std::pair<T, HistogramCountType>> value_distribution_from_segment(
    const BaseSegment& segment, const std::optional<StringHistogramDomain>& string_domain = std::nullopt);

template <typename T>
std::vector<std::pair<T, HistogramCountType>> value_distribution_from_column(
    const Table& table, const ColumnID column_id,
    const std::optional<StringHistogramDomain>& string_domain = std::nullopt);

}  // namespace histogram

}  // namespace opossum

#include "histogram_utils.ipp"