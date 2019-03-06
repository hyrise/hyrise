#pragma once

#include <memory>
#include <vector>

#include "generic_histogram.hpp"

namespace opossum {

/**
 * Utility to build GenericHistograms.
 * Bin boundary sanity checks are performed during building to facilitate debugging (i.e., debugger will break exactly
 * when an invalid bin is created and not only when the final histogram is created).
 */
template <typename T>
class GenericHistogramBuilder {
 public:
  explicit GenericHistogramBuilder(const size_t reserve_bin_count = 0, const HistogramDomain<T>& domain = {});

  // @return Whether no bins have been added so far
  bool empty() const;

  void add_bin(const T& min, const T& max, float height, float distinct_count);

  // Add a slice from another histogram's bin. slice_min and slice_max will be capped at the bin boundaries
  void add_sliced_bin(const AbstractHistogram<T>& source, const BinID bin_id, const T& slice_min, const T& slice_max);

  // Copy bins from another histogram from begin_bin_id (inclusive) to end_bin_id (exclusive)
  void add_copied_bins(const AbstractHistogram<T>& source, const BinID begin_bin_id, const BinID end_bin_id);

  // Moves all collected data into a GenericHistogram object. The GenericHistogramBuilder is expired after this call.
  std::shared_ptr<GenericHistogram<T>> build();

 private:
  HistogramDomain<T> _domain;
  std::vector<T> _bin_minima;
  std::vector<T> _bin_maxima;
  std::vector<HistogramCountType> _bin_heights;
  std::vector<HistogramCountType> _bin_distinct_counts;
};

}  // namespace opossum
