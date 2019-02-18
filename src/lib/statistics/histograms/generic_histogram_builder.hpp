#pragma once

#include <memory>
#include <vector>

#include "generic_histogram.hpp"

namespace opossum {

template <typename T>
class GenericHistogramBuilder {
 public:
  explicit GenericHistogramBuilder(const size_t reserve_bin_count = 0,
                                   const std::optional<StringHistogramDomain>& string_domain = std::nullopt);

  void add_bin(const T& min, const T& max, float height, float distinct_count);
  void add_sliced_bin(const AbstractHistogram<T>& source, const BinID bin_id, const T& slice_min, const T& slice_max);
  void add_copied_bins(const AbstractHistogram<T>& source, const BinID begin_bin_id, const BinID end_bin_id);

  std::shared_ptr<GenericHistogram<T>> build();

 private:
  BinID _current_bin_id{0};

  std::vector<T> bin_minima;
  std::vector<T> bin_maxima;
  std::vector<HistogramCountType> bin_heights;
  std::vector<HistogramCountType> bin_distinct_counts;
};

}  // namespace opossum