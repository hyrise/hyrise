#pragma once

#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "histogram_domain.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;
class BaseSegment;
template <typename T>
class GenericHistogram;
class Table;

using HistogramCountType = float;

/**
 * Returns the power of base to exp.
 * The standard library function works on doubles and is inaccurate for large numbers.
 */
uint64_t ipow(uint64_t base, uint64_t exp);

namespace histogram {

template <typename T>
std::vector<std::pair<T, HistogramCountType>> value_distribution_from_segment(
    const BaseSegment& segment, const HistogramDomain<T>& domain = {});

template <typename T>
std::vector<std::pair<T, HistogramCountType>> value_distribution_from_column(
    const Table& table, const ColumnID column_id,
    const HistogramDomain<T>& domain = {});

}  // namespace histogram

}  // namespace opossum

#include "histogram_utils.ipp"