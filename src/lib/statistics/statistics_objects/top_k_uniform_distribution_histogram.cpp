#include "top_k_uniform_distribution_histogram.hpp"

#include <cmath>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>
#include <iterator>
#include <algorithm>

#include <tsl/robin_map.h>  // NOLINT

#include "generic_histogram.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"

#include "expression/evaluation/like_matcher.hpp"
#include "generic_histogram.hpp"
#include "generic_histogram_builder.hpp"
#include "lossy_cast.hpp"
#include "resolve_type.hpp"
#include "statistics/statistics_objects/abstract_statistics_object.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "abstract_histogram.hpp"
#include "equal_distinct_count_histogram.hpp"
#include "equal_distinct_count_histogram.cpp"


namespace opossum {

template <typename T>
std::shared_ptr<GenericHistogram<T>> TopKUniformDistributionHistogram<T>::from_column(
    const Table& table, const ColumnID column_id, const BinID max_bin_count, const HistogramDomain<T>& domain) {
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero ");

  auto value_distribution = value_distribution_from_column(table, column_id, domain);

  if (value_distribution.empty()) {
    return nullptr;
  }

  // If the column holds less than K distinct values use the distinct count as TOP_K instead

  const auto k = std::min(TOP_K_DEFAULT, value_distribution.size());

  // Get the first top k values and save them into vectors
  std::vector<T> top_k_names(k);
  std::vector<HistogramCountType> top_k_counts(k);

  // Sort values by occurrence count
  auto sorted_count_values = value_distribution;
  std::sort(sorted_count_values.begin(), sorted_count_values.end(),
            [&](const auto& l, const auto& r) { return l.second > r.second; });

  // Sort TOP_K values with highest occurrence count lexicographically.
  // We later use this for more performant range predicate evaluation.
  std::sort(sorted_count_values.begin(), sorted_count_values.begin() + k,
            [&](const auto& l, const auto& r) { return l.first < r.first; });

  if (!sorted_count_values.empty()) {
    for(auto i = 0u; i < k; i++) {
      top_k_names[i] = sorted_count_values[i].first;
      top_k_counts[i] = sorted_count_values[i].second;
    }
  }

  // Remove TOP_K values from value distribution
  for (auto i = 0u; i < k; i++) {
    auto it = remove(value_distribution.begin(), value_distribution.end(), std::make_pair(top_k_names[i], top_k_counts[i]));
    value_distribution.erase(it, value_distribution.end());
  }

  // Model uniform distribution as one bin for all non-Top K values.
  const auto bin_count = value_distribution.size() < 1 ? BinID{top_k_names.size()} : BinID{2*top_k_names.size() + 1};

  GenericHistogramBuilder<T> builder{bin_count, domain};

  // Split values evenly among bins.
  //const auto range_minimum = value_distribution.front().first;
  
  const auto non_top_k_count = std::accumulate(
        value_distribution.cbegin(), 
        value_distribution.cend(), 
        HistogramCountType{0}, 
        [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; });

  const auto bin_distinct_count = value_distribution.size();
  const auto count_per_non_top_k_value = bin_distinct_count != 0 ? non_top_k_count / bin_distinct_count : BinID{0};

  auto current_minimum_index = 0u;
  auto current_maximum_index = value_distribution.size() - 1;

  for (auto top_k_index = 0u, top_k_size = top_k_names.size(); top_k_index < top_k_size; top_k_index++) {
    // current top_k value
    auto skip_bin = false;
    const auto current_top_k_value = top_k_names[top_k_index];

    // find value smaller than current top_k value for current maximum
    auto value_dist_lower_bound = std::lower_bound(value_distribution.begin(), value_distribution.end(), current_top_k_value,
      [](const auto value_count_pair, auto value) {
        return value_count_pair.first < value;
      });

    // can't build a bin if top_k value bigger than all values (== value_distribution.end()) or if there is no value smaller than top-k
    if (value_dist_lower_bound == value_distribution.end() || value_dist_lower_bound == value_distribution.begin() ||  
      std::prev(value_dist_lower_bound) - value_distribution.begin() < current_minimum_index) {
      skip_bin = true;
    } 
    // find out how many values are before current top k value
    if (!skip_bin) {
      current_maximum_index = std::prev(value_dist_lower_bound) - value_distribution.begin();
      auto current_distinct_values = current_maximum_index - current_maximum_index + 1;

      auto current_bin_height = current_distinct_values * count_per_non_top_k_value;

      // add bin with values before top_k
      builder.add_bin(
        value_distribution[current_minimum_index].first, 
        value_distribution[current_maximum_index].first, 
        current_bin_height, 
        current_distinct_values
      );
    }
    
    // add bin for topk value
    builder.add_bin(
      current_top_k_value, 
      current_top_k_value, 
      top_k_counts[top_k_index], 
      1
    );

    // advance minimum pointer

    current_minimum_index = value_dist_lower_bound - value_distribution.begin();
    
  }
      // add last bucket if necessary
  if (current_minimum_index <= value_distribution.size() - 1 && value_distribution.size() > 0) {
    const auto range_maximum = value_distribution.back().first;
    auto current_distinct_values = value_distribution.size() - current_maximum_index;
    auto current_bin_height = current_distinct_values * count_per_non_top_k_value;
    builder.add_bin(value_distribution[current_minimum_index].first, range_maximum, current_bin_height, current_distinct_values);    
  }


  return builder.build();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(TopKUniformDistributionHistogram);

}  // namespace opossum
