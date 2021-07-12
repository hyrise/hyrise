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
TopKUniformDistributionHistogram<T>::TopKUniformDistributionHistogram(std::shared_ptr<GenericHistogram<T>> histogram, 
  std::vector<T>&& top_k_names, std::vector<HistogramCountType>&& top_k_counts, const HistogramDomain<T>& domain)
    : AbstractHistogram<T>(domain), _histogram(histogram), _top_k_names(top_k_names), _top_k_counts(top_k_counts) {

  // TODO: moved to sliced method
  if (_histogram == nullptr) {
    GenericHistogramBuilder<T> builder{0, AbstractHistogram<T>::domain()};
    _histogram = builder.build();
  }

  const auto histogram_total_count = _histogram->total_count();
  const auto top_k_total_count = std::accumulate(_top_k_counts.cbegin(), _top_k_counts.cend(), HistogramCountType{0});
  _total_count = histogram_total_count + top_k_total_count;
  _total_distinct_count = _histogram->total_distinct_count();

}

template <typename T>
std::shared_ptr<TopKUniformDistributionHistogram<T>> TopKUniformDistributionHistogram<T>::from_column(
    const Table& table, const ColumnID column_id, const BinID max_bin_count, const HistogramDomain<T>& domain) {
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero ");

  auto value_distribution = value_distribution_from_column(table, column_id, domain);

  if (value_distribution.empty()) {
    return nullptr;
  }

  // If the column holds less than K distinct values use the distinct count as TOP_K instead

  auto k = std::min(TOP_K_DEFAULT, value_distribution.size());

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

  //print top k values

  // for(auto i = 0u; i < TOP_K; i++) {
  //       std::cout << "value: " << top_k_names[i] << " with count: " << top_k_counts[i] <<std::endl;
  // }

  // Remove TOP_K values from value distribution
  for (auto i = 0u; i < k; i++) {
    auto it = remove(value_distribution.begin(), value_distribution.end(), std::make_pair(top_k_names[i], top_k_counts[i]));
    value_distribution.erase(it, value_distribution.end());
  }

  // Model uniform distribution as one bin for all non-Top K values.
  const auto bin_count = value_distribution.size() < 1 ? BinID{0} : BinID{1};

  GenericHistogramBuilder<T> builder{bin_count, domain};

  if (bin_count > 0) {

    // Split values evenly among bins.
    const auto bin_minimum = value_distribution.front().first;
    const auto bin_maximum = value_distribution.back().first;
    const auto bin_height = std::accumulate(
          value_distribution.cbegin(), 
          value_distribution.cend(), 
          HistogramCountType{0}, 
          [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; });

    const auto bin_distinct_count = value_distribution.size();

  
    builder.add_bin(bin_minimum, bin_maximum, bin_height, bin_distinct_count);  
  }

  auto histogram = builder.build();

  return std::make_shared<TopKUniformDistributionHistogram<T>>(histogram, std::move(top_k_names), std::move(top_k_counts));
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> TopKUniformDistributionHistogram<T>::sliced(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {

  // if (AbstractHistogram<T>::does_not_contain(predicate_condition, variant_value, variant_value2)) {
  //   return nullptr;
  // }

  const auto value = lossy_variant_cast<T>(variant_value);
  DebugAssert(value, "sliced() cannot be called with NULL");

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      GenericHistogramBuilder<T> builder{1, AbstractHistogram<T>::domain()};
      bool is_top_k_value = false; 
      for (auto i = 0u; i < _top_k_names.size(); i++) {
        if(_top_k_names[i] == value) {
          is_top_k_value = true;
          builder.add_bin(*value, *value, _top_k_counts[i], 1);
          break;
        }
      }
      if(!is_top_k_value) {
        builder.add_bin(*value, *value,
                        static_cast<HistogramCountType>(AbstractHistogram<T>::estimate_cardinality(PredicateCondition::Equals, variant_value)),
                        1);
      }
      return builder.build();
    }
    case PredicateCondition::NotEquals: {
      // Check if value in top-k values
      auto top_k_index = _top_k_names.size();
      
      for (auto i = 0u; i < _top_k_names.size(); i++) {
        if(_top_k_names[i] == value) {
          top_k_index = i;
          break;
        }
      }

      auto new_top_k_names = _top_k_names;
      auto new_top_k_counts = _top_k_counts;
      auto new_histogram = std::static_pointer_cast<GenericHistogram<T>>(_histogram->clone());

      if (top_k_index != _top_k_names.size()) {
        // Value in Top-K Values
        new_top_k_names.erase(new_top_k_names.begin() + top_k_index);
        new_top_k_counts.erase(new_top_k_counts.begin() + top_k_index);

      } else {
        // Value not in Top-K Values
        new_histogram = std::static_pointer_cast<GenericHistogram<T>>(_histogram->sliced(predicate_condition, variant_value, variant_value2));
      }

      return std::make_shared<TopKUniformDistributionHistogram<T>>(new_histogram, std::move(new_top_k_names), std::move(new_top_k_counts));
    }
    case PredicateCondition::LessThanEquals: {
      // Top-K Values
      // TODO: is copy necessary?
      auto new_top_k_names = _top_k_names;
      auto new_top_k_counts = _top_k_counts;

      auto upper_bound = std::upper_bound(new_top_k_names.begin(), new_top_k_names.end(), value);
      new_top_k_names.erase(upper_bound, new_top_k_names.end());
      new_top_k_counts.resize(new_top_k_names.size());

      //print new top k values
      // std::cout << "New top k values after applying PredicateCondition::LessThanEquals" << std::endl;
      // for(auto i = 0u; i < new_top_k_names.size(); i++) {
      //   std::cout << "value: " << new_top_k_names[i] << " with count: " << new_top_k_counts[i] << std::endl;
      // }

      // Histogram Values
      auto new_histogram = std::static_pointer_cast<GenericHistogram<T>>(_histogram->sliced(predicate_condition, variant_value, variant_value2));

      return std::make_shared<TopKUniformDistributionHistogram<T>>(new_histogram, std::move(new_top_k_names), std::move(new_top_k_counts));
    }
    case PredicateCondition::LessThan: {
      // Top-K Values
      auto new_top_k_names = _top_k_names;
      auto new_top_k_counts = _top_k_counts;

      auto lower_bound = std::lower_bound(new_top_k_names.begin(), new_top_k_names.end(), value);
      new_top_k_names.erase(lower_bound, new_top_k_names.end());
      new_top_k_counts.resize(new_top_k_names.size());

      //print new top k values
      // std::cout << "New top k values after applying PredicateCondition::LessThan" << std::endl;
      // for(auto i = 0u; i < new_top_k_names.size(); i++) {
      //   std::cout << "value: " << new_top_k_names[i] << " with count: " << new_top_k_counts[i] << std::endl;
      // }

      // Histogram Values
      auto new_histogram = std::static_pointer_cast<GenericHistogram<T>>(_histogram->sliced(predicate_condition, variant_value, variant_value2));

      return std::make_shared<TopKUniformDistributionHistogram<T>>(new_histogram, std::move(new_top_k_names), std::move(new_top_k_counts));
    }
    case PredicateCondition::GreaterThan: {
      // Top-K Values
      auto new_top_k_names = _top_k_names;
      auto new_top_k_counts = _top_k_counts;

      auto upper_bound = std::upper_bound(new_top_k_names.begin(), new_top_k_names.end(), value);

      const auto previous_top_k_names_size = new_top_k_names.size();
      new_top_k_names.erase(new_top_k_names.begin(), upper_bound);
      const auto num_deleted_top_k_values = previous_top_k_names_size - new_top_k_names.size();
      new_top_k_counts.erase(new_top_k_counts.begin(), new_top_k_counts.begin() + num_deleted_top_k_values);

      //print new top k values
      // std::cout << "New top k values after applying PredicateCondition::GreaterThan" << std::endl;
      // for(auto i = 0u; i < new_top_k_names.size(); i++) {
      //   std::cout << "value: " << new_top_k_names[i] << " with count: " << new_top_k_counts[i] << std::endl;
      // }

      // Histogram Values
      auto new_histogram = std::static_pointer_cast<GenericHistogram<T>>(_histogram->sliced(predicate_condition, variant_value, variant_value2));

      return std::make_shared<TopKUniformDistributionHistogram<T>>(new_histogram, std::move(new_top_k_names), std::move(new_top_k_counts));
    }
    case PredicateCondition::GreaterThanEquals: {
      // Top-K Values
      auto new_top_k_names = _top_k_names;
      auto new_top_k_counts = _top_k_counts;

      auto lower_bound = std::lower_bound(new_top_k_names.begin(), new_top_k_names.end(), value);

      const auto previous_top_k_names_size = new_top_k_names.size();
      new_top_k_names.erase(new_top_k_names.begin(), lower_bound);
      const auto num_deleted_top_k_values = previous_top_k_names_size - new_top_k_names.size();
      new_top_k_counts.erase(new_top_k_counts.begin(), new_top_k_counts.begin() + num_deleted_top_k_values);

      //print new top k values
      // std::cout << "New top k values after applying PredicateCondition::GreaterThanEquals" << std::endl;
      // for(auto i = 0u; i < new_top_k_names.size(); i++) {
      //   std::cout << "value: " << new_top_k_names[i] << " with count: " << new_top_k_counts[i] << std::endl;
      // }

      // Histogram Values
      auto new_histogram = std::static_pointer_cast<GenericHistogram<T>>(_histogram->sliced(predicate_condition, variant_value, variant_value2));

      return std::make_shared<TopKUniformDistributionHistogram<T>>(new_histogram, std::move(new_top_k_names), std::move(new_top_k_counts));
    }
    case PredicateCondition::BetweenInclusive:
      Assert(variant_value2, "BETWEEN needs a second value.");
      return std::static_pointer_cast<TopKUniformDistributionHistogram<T>>(sliced(PredicateCondition::GreaterThanEquals, variant_value, variant_value2))
          ->sliced(PredicateCondition::LessThanEquals, *variant_value2, variant_value2);
    case PredicateCondition::BetweenLowerExclusive:
      Assert(variant_value2, "BETWEEN needs a second value.");
      return std::static_pointer_cast<TopKUniformDistributionHistogram<T>>(sliced(PredicateCondition::GreaterThan, variant_value, variant_value2))
          ->sliced(PredicateCondition::LessThanEquals, *variant_value2, variant_value2);
    case PredicateCondition::BetweenUpperExclusive:
      Assert(variant_value2, "BETWEEN needs a second value.");
      return std::static_pointer_cast<TopKUniformDistributionHistogram<T>>(sliced(PredicateCondition::GreaterThanEquals, variant_value, variant_value2))
          ->sliced(PredicateCondition::LessThan, *variant_value2, variant_value2);
    case PredicateCondition::BetweenExclusive:
      Assert(variant_value2, "BETWEEN needs a second value.");
      return std::static_pointer_cast<TopKUniformDistributionHistogram<T>>(sliced(PredicateCondition::GreaterThan, variant_value, variant_value2))
          ->sliced(PredicateCondition::LessThan, *variant_value2, variant_value2);
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      // TODO(anybody) Slicing for (NOT) LIKE not supported, yet
      return clone();
    case PredicateCondition::In:
    case PredicateCondition::NotIn:
    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      Fail("PredicateCondition not supported by TopKUnifromDistributionHistogram");
  }
  
  Fail("Invalid enum value");
}


template <typename T>
std::string TopKUniformDistributionHistogram<T>::name() const {
  return "TopKEqualDistinctCount";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> TopKUniformDistributionHistogram<T>::clone() const {
  // The new histogram needs a copy of the data
  auto histogram_copy = std::static_pointer_cast<GenericHistogram<T>>(_histogram->clone());
  auto top_k_names_copy = _top_k_names;
  auto top_k_counts_copy = _top_k_counts;
  //return histogram_copy;
  return std::make_shared<TopKUniformDistributionHistogram<T>>(histogram_copy, std::move(top_k_names_copy), std::move(top_k_counts_copy));
}

template <typename T>
BinID TopKUniformDistributionHistogram<T>::bin_count() const {
  return _histogram->bin_count();
}

template <typename T>
BinID TopKUniformDistributionHistogram<T>::_bin_for_value(const T& value) const {
  //TODO: look at where is that used
  for (auto i = 0u; i < _top_k_names.size(); i++) {
    if(_top_k_names[i] == value) {
      return 1;
    }
  }
  return _histogram->bin_for_value(value);
}

template <typename T>
BinID TopKUniformDistributionHistogram<T>::_next_bin_for_value(const T& value) const {
  //TODO: look at where is that used
  for (auto i = 0u; i < _top_k_names.size(); i++) {
    if(_top_k_names[i] == value) {
      return 1;
    }
  }
  return _histogram->next_bin_for_value(value);
}

template <typename T>
const T& TopKUniformDistributionHistogram<T>::bin_minimum(const BinID index) const {
  return _histogram->bin_minimum(index);
}

template <typename T>
const T& TopKUniformDistributionHistogram<T>::bin_maximum(const BinID index) const {
  return _histogram->bin_maximum(index);
}

template <typename T>
HistogramCountType TopKUniformDistributionHistogram<T>::bin_height(const BinID index) const {
  return _histogram->bin_height(index);
}

template <typename T>
HistogramCountType TopKUniformDistributionHistogram<T>::bin_distinct_count(const BinID index) const {
  return _histogram->bin_distinct_count(index);
}

template <typename T>
HistogramCountType TopKUniformDistributionHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
HistogramCountType TopKUniformDistributionHistogram<T>::total_distinct_count() const {
  return _total_distinct_count;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(TopKUniformDistributionHistogram);

}  // namespace opossum
