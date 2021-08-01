#include "top_k_uniform_distribution_histogram.hpp"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "equal_distinct_count_histogram.cpp"
#include "equal_distinct_count_histogram.hpp"
#include "generic_histogram_builder.hpp"

namespace opossum {

template <typename T>
std::shared_ptr<GenericHistogram<T>> TopKUniformDistributionHistogram<T>::from_column(
    const Table& table, const ColumnID column_id, const HistogramDomain<T>& domain) {
  auto value_distribution = value_distribution_from_column(table, column_id, domain);

  if (value_distribution.empty()) {
    return nullptr;
  }

  // If the column holds less than K distinct values use the distinct count as TOP_K instead
  const auto k = std::min(TOP_K_DEFAULT, value_distribution.size());

  // Get the first Top K values and save them into vectors
  std::vector<T> top_k_names(k);
  std::vector<HistogramCountType> top_k_counts(k);

  // Sort values in value distribution by occurrence count
  auto sorted_value_counts = value_distribution;
  std::sort(sorted_value_counts.begin(), sorted_value_counts.end(),
            [&](const auto& left_value_count, const auto& right_value_count) {
              return left_value_count.second > right_value_count.second;
            });

  // Sort Top K values lexicographically
  // We later use the lexicographically sorted Top K values for easier and more performant histogram construction
  std::sort(sorted_value_counts.begin(), sorted_value_counts.begin() + k,
            [&](const auto& left_value_count, const auto& right_value_count) {
              return left_value_count.first < right_value_count.first;
            });

  for (auto top_k_index = 0u; top_k_index < k; top_k_index++) {
    top_k_names[top_k_index] = sorted_value_counts[top_k_index].first;
    top_k_counts[top_k_index] = sorted_value_counts[top_k_index].second;
  }

  // Remove Top K values from value distribution
  for (auto top_k_index = 0u; top_k_index < k; top_k_index++) {
    auto value_distribution_it = remove(value_distribution.begin(), value_distribution.end(),
                                        std::make_pair(top_k_names[top_k_index], top_k_counts[top_k_index]));
    value_distribution.erase(value_distribution_it, value_distribution.end());
  }

  // Each Top K value is modeled as one bin with height as its stored count.
  // Between two Top K value bins, one bin is created for the non-Top K values between them.
  // Together with a potential last bin after the last Top K value we have a maximum bin count of 2 * K + 1
  // If there are no more values stored in value_distribution after the Top K values have been removed, we only have Top K values and therefore need exactly k bins
  const auto bin_count = value_distribution.size() < 1 ? BinID{k} : BinID{2 * top_k_names.size() + 1};

  GenericHistogramBuilder<T> builder{bin_count, domain};

// Calculate estimate for occurrence count for Non-Top K values assuming a uniform distribution
  const auto non_top_k_count =
      std::accumulate(value_distribution.cbegin(), value_distribution.cend(), HistogramCountType{0},
                      [](HistogramCountType current_count, const std::pair<T, HistogramCountType>& value_count) {
                        return current_count + value_count.second;
                      });

  const auto bin_distinct_count = value_distribution.size();
  const auto count_per_non_top_k_value = bin_distinct_count != 0 ? non_top_k_count / bin_distinct_count : BinID{0};

// Construct Generic Histogram with single bins for Top-K Values
// For Non-Top K Values, one bin is created for all non-Top K values between two Top K bins using the calculated estimation of count_per_non_top_k_value
  auto current_minimum_index = 0u;
  auto current_maximum_index = value_distribution.size() - 1;

  for (auto top_k_index = 0ul, top_k_size = top_k_names.size(); top_k_index < top_k_size; top_k_index++) {
    const auto current_top_k_value = top_k_names[top_k_index];

// For each Top K value a Non-Top K values bin between the previous Top K value and itself, as well as a Top K value bin for itself are created
    // find maximum value that is still smaller than current top_k value
    auto value_dist_lower_bound =
        std::lower_bound(value_distribution.begin(), value_distribution.end(), current_top_k_value,
                         [](const auto value_count_pair, auto value) { return value_count_pair.first < value; });

    // Add a non-Top K value bin and a top k value bin
    // We can skip a non Top K value bin, if there are no non-Top K values between two Top K value bins
    if (!(value_dist_lower_bound == value_distribution.begin() ||
          std::prev(value_dist_lower_bound) - value_distribution.begin() < current_minimum_index)) {
      // find out how many values are before current Top K value
      current_maximum_index = std::prev(value_dist_lower_bound) - value_distribution.begin();
      const auto current_distinct_values = current_maximum_index - current_minimum_index + 1;
      const auto current_bin_height = current_distinct_values * count_per_non_top_k_value;

      // add bin with values before Top K
      builder.add_bin(value_distribution[current_minimum_index].first, value_distribution[current_maximum_index].first,
                      current_bin_height, current_distinct_values);
    }

    // add bin for Top K value
    builder.add_bin(current_top_k_value, current_top_k_value, top_k_counts[top_k_index], 1);

    // advance minimum index
    current_minimum_index = value_dist_lower_bound - value_distribution.begin();
  }

  // add last bucket if non Top K values are still left after the last Top K value
  if (current_minimum_index <= value_distribution.size() - 1 && value_distribution.size() > 0) {
    const auto range_maximum = value_distribution.back().first;
    const auto current_distinct_values = value_distribution.size() - 1 - current_maximum_index;
    const auto current_bin_height = current_distinct_values * count_per_non_top_k_value;
    builder.add_bin(value_distribution[current_minimum_index].first, range_maximum, current_bin_height,
                    current_distinct_values);
  }

  return builder.build();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(TopKUniformDistributionHistogram);

}  // namespace opossum
