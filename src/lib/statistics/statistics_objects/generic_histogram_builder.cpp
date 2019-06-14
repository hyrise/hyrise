#include "generic_histogram_builder.hpp"

#include "utils/assert.hpp"

namespace opossum {

template <typename T>
GenericHistogramBuilder<T>::GenericHistogramBuilder(const size_t reserve_bin_count, const HistogramDomain<T>& domain)
    : _domain(domain) {
  _bin_minima.reserve(reserve_bin_count);
  _bin_maxima.reserve(reserve_bin_count);
  _bin_heights.reserve(reserve_bin_count);
  _bin_distinct_counts.reserve(reserve_bin_count);
}

template <typename T>
bool GenericHistogramBuilder<T>::empty() const {
  return _bin_minima.empty();
}

template <typename T>
void GenericHistogramBuilder<T>::add_bin(const T& min, const T& max, float height, float distinct_count) {
  DebugAssert(_bin_minima.empty() || min > _bin_maxima.back(), "Bins must be sorted and cannot overlap");
  DebugAssert(min <= max, "Invalid bin slice");

  _bin_minima.emplace_back(min);
  _bin_maxima.emplace_back(max);
  _bin_heights.emplace_back(static_cast<HistogramCountType>(height));
  _bin_distinct_counts.emplace_back(static_cast<HistogramCountType>(distinct_count));
}

template <typename T>
void GenericHistogramBuilder<T>::add_sliced_bin(const AbstractHistogram<T>& source, const BinID bin_id,
                                                const T& slice_min, const T& slice_max) {
  DebugAssert(slice_max >= slice_min, "Invalid slice");
  DebugAssert(slice_min >= source.bin_minimum(bin_id), "Invalid slice minimum");
  DebugAssert(slice_max <= source.bin_maximum(bin_id), "Invalid slice maximum");

  const auto sliced_bin_ratio =
      source.bin_ratio_less_than_equals(bin_id, slice_max) - source.bin_ratio_less_than(bin_id, slice_min);

  auto height = source.bin_height(bin_id) * sliced_bin_ratio;
  auto distinct_count = source.bin_distinct_count(bin_id) * sliced_bin_ratio;

  add_bin(slice_min, slice_max, height, distinct_count);
}

template <typename T>
void GenericHistogramBuilder<T>::add_copied_bins(const AbstractHistogram<T>& source, const BinID begin_bin_id,
                                                 const BinID end_bin_id) {
  DebugAssert(begin_bin_id <= end_bin_id, "Invalid bin range");

  for (auto bin_id = begin_bin_id; bin_id < end_bin_id; ++bin_id) {
    const auto bin = source.bin(bin_id);
    add_bin(bin.min, bin.max, bin.height, bin.distinct_count);
  }
}

template <typename T>
std::shared_ptr<GenericHistogram<T>> GenericHistogramBuilder<T>::build() {
  return std::make_shared<GenericHistogram<T>>(std::move(_bin_minima), std::move(_bin_maxima), std::move(_bin_heights),
                                               std::move(_bin_distinct_counts), _domain);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(GenericHistogramBuilder);

}  // namespace opossum
