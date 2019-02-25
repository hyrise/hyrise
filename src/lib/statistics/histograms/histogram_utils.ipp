#include "abstract_histogram.hpp"

#include "generic_histogram.hpp"
#include "generic_histogram_builder.hpp"
#include "storage/base_segment.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

namespace histogram {

namespace detail {

template <typename T>
std::unordered_map<T, HistogramCountType> value_distribution_from_segment_impl(
    const BaseSegment& segment, std::unordered_map<T, HistogramCountType> value_distribution,
    const HistogramDomain<T>& domain) {
  segment_iterate<T>(segment, [&](const auto& iterator_value) {
    if (!iterator_value.is_null()) {
      if constexpr (std::is_same_v<T, std::string>) {
        if (domain.contains(iterator_value.value())) {
          ++value_distribution[iterator_value.value()];
        } else {
          ++value_distribution[domain.string_to_domain(iterator_value.value())];
        }
      } else {
        ++value_distribution[iterator_value.value()];
      }
    }
  });

  return value_distribution;
}

}  // namespace detail

template <typename T>
std::vector<std::pair<T, HistogramCountType>> value_distribution_from_segment(const BaseSegment& segment,
                                                                              const HistogramDomain<T>& domain) {
  auto value_distribution_map = detail::value_distribution_from_segment_impl<T>(segment, {}, domain);

  auto value_distribution =
      std::vector<std::pair<T, HistogramCountType>>{value_distribution_map.begin(), value_distribution_map.end()};
  std::sort(value_distribution.begin(), value_distribution.end(),
            [&](const auto& l, const auto& r) { return l.first < r.first; });

  return value_distribution;
}

template <typename T>
std::vector<std::pair<T, HistogramCountType>> value_distribution_from_column(const Table& table,
                                                                             const ColumnID column_id,
                                                                             const HistogramDomain<T>& domain) {
  std::unordered_map<T, HistogramCountType> value_distribution_map;

  for (const auto& chunk : table.chunks()) {
    value_distribution_map = detail::value_distribution_from_segment_impl<T>(*chunk->get_segment(column_id),
                                                                             std::move(value_distribution_map), domain);
  }

  auto value_distribution =
      std::vector<std::pair<T, HistogramCountType>>{value_distribution_map.begin(), value_distribution_map.end()};
  std::sort(value_distribution.begin(), value_distribution.end(),
            [&](const auto& l, const auto& r) { return l.first < r.first; });

  return value_distribution;
}

}  // namespace histogram

}  // namespace opossum