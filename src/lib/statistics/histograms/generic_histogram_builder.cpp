#include "generic_histogram_builder.hpp"

#include "utils/assert.hpp"

namespace opossum {

template<typename T>
GenericHistogramBuilder<T>::GenericHistogramBuilder(const size_t reserve_bin_count, const std::optional<StringHistogramDomain>& string_domain) {
  constexpr auto is_string_histogram = std::is_same_v<T, std::string>; // Cannot do this in the first Assert arg... :(
  Assert(is_string_histogram == string_domain.has_value(), "StringHistogramDomain required IFF T == std::string");

  bin_minima.reserve(reserve_bin_count);
  bin_maxima.reserve(reserve_bin_count);
  bin_heights.reserve(reserve_bin_count);
  bin_distinct_counts.reserve(reserve_bin_count);
}

template<typename T>
void GenericHistogramBuilder<T>::add_bin(const T& min, const T& max, float height, float distinct_count) {
  DebugAssert(bin_minima.empty() || min > bin_minima.back(), "Bins must be sorted and cannot overlap");
  DebugAssert(min <= max, "Invalid bin slice");

  /**
   * In floating point arithmetics, it is virtually impossible to write algorithms that guarantee that cardinality is
   * always greater_than_equal distinct_count. We have gone to just correcting small numerical error, sad as it is.
   */
  distinct_count = std::min(height, distinct_count);

  height = std::ceil(height);
  distinct_count = std::ceil(distinct_count);

  DebugAssert(height > 0, "Bin height cannot be zero");
  DebugAssert(distinct_count > 0, "Invalid bin distinct count");
  DebugAssert(min != max || distinct_count == 1, "Bins with equal min and max can only have one distinct value");

  if constexpr (std::is_integral_v<T>) {
    Assert(static_cast<HistogramCountType>(max + 1 - min) >= distinct_count, "Higher distinct_count than individual integer values in bin");
  }

  bin_minima.emplace_back(min);
  bin_maxima.emplace_back(max);
  bin_heights.emplace_back(static_cast<HistogramCountType>(height));
  bin_distinct_counts.emplace_back(static_cast<HistogramCountType>(distinct_count));
}

template<typename T>
void GenericHistogramBuilder<T>::add_sliced_bin(const AbstractHistogram<T>& source, const BinID bin_id, const T& slice_min, const T& slice_max) {
  DebugAssert(slice_max >= slice_min, "Invalid slice");
  DebugAssert(slice_min >= source.bin_minimum(bin_id), "Invalid slice minimum");
  DebugAssert(slice_max <= source.bin_maximum(bin_id), "Invalid slice minimum");

  const auto sliced_bin_ratio = source.bin_ratio_less_than(bin_id, source.get_next_value(slice_max)) - source.bin_ratio_less_than(bin_id, slice_min);

  auto height = source.bin_height(bin_id) * sliced_bin_ratio;
  auto distinct_count = source.bin_distinct_count(bin_id) * sliced_bin_ratio;

  // Floating point quirk:
  // `min == max` (resulting in sliced_bin_ratio == 0.0f) could happen with `distinct_count != 1`, e.g., when slicing
  // [2, next_value(2)] to [2, 2]
  if (slice_min == slice_max) {
    height = std::max(height, 1.0f);
    distinct_count = 1;
  }

  add_bin(slice_min, slice_max, height, distinct_count);
}

template<typename T>
void GenericHistogramBuilder<T>::add_copied_bins(const AbstractHistogram<T>& source, const BinID begin_bin_id, const BinID end_bin_id) {
  DebugAssert(begin_bin_id <= end_bin_id, "Invalid bin range");

  for (auto bin_id = begin_bin_id; bin_id < end_bin_id; ++bin_id) {
    const auto bin = source.bin(bin_id);
    add_bin(bin.min, bin.max, bin.height, bin.distinct_count);
  }
}

template<typename T>
std::shared_ptr<GenericHistogram<T>> GenericHistogramBuilder<T>::build() {
  return std::make_shared<GenericHistogram<T>>(std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
                                               std::move(bin_distinct_counts));
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(GenericHistogramBuilder);

}  // namespace opossum
