#include "abstract_histogram.hpp"

#include "generic_histogram.hpp"
#include "generic_histogram_builder.hpp"

namespace opossum {

namespace histogram {

template <typename T>
std::shared_ptr<GenericHistogram<T>> merge_histograms(const AbstractHistogram<T>& histogram_a,
                                                       const AbstractHistogram<T>& histogram_b) {
  if constexpr (std::is_same_v<T, std::string>) {
    Assert(histogram_a.string_domain() == histogram_b.string_domain(), "Cannot merge string histograms with different domains");
  }

  GenericHistogramBuilder<T> builder{std::max(histogram_a.bin_count(), histogram_b.bin_count()), histogram_a.string_domain()};

  const auto get_ratio_of_bin = [](const AbstractHistogram<T>& histogram, const size_t bin_idx, const T& min,
                                   const T& max) -> float {
    return histogram.bin_ratio_less_than_equals(bin_idx, max) -
           histogram.bin_ratio_less_than(bin_idx, min);
  };

  auto current_min = std::min(histogram_a.bin_minimum(BinID{0}), histogram_b.bin_minimum(BinID{0}));

  auto bin_idx_a = BinID{0};
  auto bin_idx_b = BinID{0};

  while (bin_idx_a < histogram_a.bin_count() && bin_idx_b < histogram_b.bin_count()) {
    const auto min_a = histogram_a.bin_minimum(bin_idx_a);
    const auto max_a = histogram_a.bin_maximum(bin_idx_a);
    const auto min_b = histogram_b.bin_minimum(bin_idx_b);
    const auto max_b = histogram_b.bin_maximum(bin_idx_b);

    current_min = std::max(current_min, std::min({min_a, min_b}));
    auto current_max = T{};
    auto height = float{};
    auto distinct_count = float{};

    if (current_min < min_b) {
      // Bin A only

      if constexpr (std::is_same_v<T, std::string>) {
        current_max = std::min(min_b, max_a);
      } else {
        current_max = std::min(previous_value(min_b), max_a);
      }

      builder.add_sliced_bin(histogram_a, bin_idx_a, current_min, current_max);

    } else if (current_min < min_a) {
      // Bin B only

      if constexpr (std::is_same_v<T, std::string>) {
        current_max = std::min(min_a, max_b);
      } else {
        current_max = std::min(previous_value(min_a), max_b);
      }

      builder.add_sliced_bin(histogram_b, bin_idx_b, current_min, current_max);

    } else {
      // From both

      current_max = std::min(max_a, max_b);

      const auto ratio_a = get_ratio_of_bin(histogram_a, bin_idx_a, current_min, current_max);
      const auto ratio_b = get_ratio_of_bin(histogram_b, bin_idx_b, current_min, current_max);
      height = histogram_a.bin_height(bin_idx_a) * ratio_a + histogram_b.bin_height(bin_idx_b) * ratio_b;
      distinct_count = histogram_a.bin_distinct_count(bin_idx_a) * ratio_a + histogram_b.bin_distinct_count(bin_idx_b) * ratio_b;

      builder.add_bin(current_min, current_max, height, distinct_count);
    }

    if (current_max == max_a) {
      ++bin_idx_a;
    }

    if (current_max == max_b) {
      ++bin_idx_b;
    }

    if constexpr (std::is_same_v<T, std::string>) {
      current_min = histogram_a.string_domain()->value_after(current_max);
    } else {
      current_min = histogram_a.get_next_value(current_max);
    }
  }

  for (; bin_idx_a < histogram_a.bin_count(); ++bin_idx_a) {
    current_min = std::max(current_min, histogram_a.bin_minimum(bin_idx_a));
    const auto current_max = histogram_a.bin_maximum(bin_idx_a);

    builder.add_sliced_bin(histogram_a, bin_idx_a, current_min, current_max);
  }

  for (; bin_idx_b < histogram_b.bin_count(); ++bin_idx_b) {
    current_min = std::max(current_min, histogram_b.bin_minimum(bin_idx_b));
    const auto current_max = histogram_b.bin_maximum(bin_idx_b);

    builder.add_sliced_bin(histogram_b, bin_idx_b, current_min, current_max);
  }

  return builder.build();
}

template <typename T>
std::shared_ptr<GenericHistogram<T>> reduce_histogram(const AbstractHistogram<T>& histogram,
                                                       const size_t max_bin_count) {

  GenericHistogramBuilder<T> builder(0, histogram.string_domain());

  // Number of consecutive bins to merge into one
  const auto reduce_factor = static_cast<BinID>(std::ceil(static_cast<float>(histogram.bin_count()) / max_bin_count));

  if (reduce_factor <= 1) {
    builder.add_copied_bins(histogram, BinID{0}, histogram.bin_count());
    return builder.build();
  }

  for (auto bin_idx = BinID{0}; bin_idx < histogram.bin_count(); bin_idx += reduce_factor) {
    const auto first_bin_idx = bin_idx;
    const auto last_bin_idx = std::min(bin_idx + reduce_factor - 1, histogram.bin_count() - 1);

    auto height = HistogramCountType{0};
    auto distinct_count = HistogramCountType{0};

    for (auto merge_bin_idx = bin_idx; merge_bin_idx <= last_bin_idx; ++merge_bin_idx) {
      height += histogram.bin_height(merge_bin_idx);
      distinct_count += histogram.bin_distinct_count(merge_bin_idx);
    }

    builder.add_bin(histogram.bin_minimum(first_bin_idx), histogram.bin_maximum(last_bin_idx), height, distinct_count);
  }

  return builder.build();
}

}  // namespace histogram

}  // namespace opossum