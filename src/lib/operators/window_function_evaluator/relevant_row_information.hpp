#pragma once

#include <compare>
#include <span>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise::window_function_evaluator {

std::weak_ordering reverse(std::weak_ordering ordering);
std::weak_ordering compare_with_null_equal(const AllTypeVariant& lhs, const AllTypeVariant& rhs);
std::weak_ordering compare_with_null_equal(std::span<const AllTypeVariant> lhs, std::span<const AllTypeVariant> rhs);

std::weak_ordering compare_with_null_equal(std::span<const AllTypeVariant> lhs, std::span<const AllTypeVariant> rhs,
                                           auto is_column_reversed) {
  DebugAssert(lhs.size() == rhs.size(), "Tried to compare rows with different column counts.");

  for (auto column_index = 0u; column_index < lhs.size(); ++column_index) {
    const auto element_ordering = compare_with_null_equal(lhs[column_index], rhs[column_index]);
    if (element_ordering != std::weak_ordering::equivalent) {
      return is_column_reversed(column_index) ? reverse(element_ordering) : element_ordering;
    }
  }

  return std::weak_ordering::equivalent;
}

struct RelevantRowInformation {
  std::vector<AllTypeVariant> partition_values;
  std::vector<AllTypeVariant> order_values;
  AllTypeVariant function_argument;
  RowID row_id;
};

}  // namespace hyrise::window_function_evaluator
