#include "equal_distinct_count_histogram.hpp"

#include <cstddef>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/sort/sort.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/histogram_domain.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/small_prefix_string_view.hpp"
#include "utils/string_heap.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// constexpr auto MIN_CHUNK_COUNT_TO_INCLUDE = ChunkID{1'000};
constexpr auto MIN_CHUNK_COUNT_TO_INCLUDE = ChunkID{10'000'000};

// Think of this as an unordered_map<T, HistogramCountType>. The hash, equals, and allocator template parameter are
// defaults so that we can set the last parameter. It controls whether the hash for a value should be cached. Doing
// so reduces the cost of rehashing at the cost of slightly higher memory consumption. We only do it for strings,
// where hashing is somewhat expensive.

template <typename T>
using ValueDistributionType = std::conditional_t<std::is_same_v<T, pmr_string>, SmallPrefixStringView, T>;
 
template <typename T>
using ValueDistributionMap = boost::unordered_flat_map<ValueDistributionType<T>, size_t>;

template <typename T>
using ValueDistributionVector = std::vector<std::pair<ValueDistributionType<T>, size_t>>;

template <typename T, typename DictionarySegmentType, typename DictionaryType>
void process_dictionary_segment(const DictionarySegmentType& segment, const DictionaryType& dictionary, ValueDistributionVector<T>& value_distribution_vector, StringHeap& string_heap, const HistogramDomain<T>& domain) {
  const auto start = std::chrono::steady_clock::now();
  const auto dictionary_size = dictionary.size();
  value_distribution_vector.resize(dictionary_size);

  for (auto dictionary_offset = size_t{0}; dictionary_offset < dictionary_size; ++dictionary_offset) {
    if constexpr (std::is_same_v<T, pmr_string>) {
      auto initial_string = std::string_view{};
      if constexpr (std::is_same_v<std::decay_t<DictionaryType>, FixedStringVector>) {
        initial_string = dictionary.template get_string_at<std::string_view>(dictionary_offset);
      } else {
        initial_string = dictionary[dictionary_offset];
      }

      const auto adapted_string = domain.string_to_domain(initial_string.data(), initial_string.size());
      if (initial_string == adapted_string) {
        value_distribution_vector[dictionary_offset] = {SmallPrefixStringView{initial_string.data(), initial_string.size()}, 0};
      } else {
        const auto heap_string_view = string_heap.add_string(adapted_string);
        value_distribution_vector[dictionary_offset] = {heap_string_view, 0};
      }
    } else {
      value_distribution_vector[dictionary_offset] = {dictionary[dictionary_offset], 0};
    }
  }

  const auto attribute_vector = segment.attribute_vector();
  const auto null_value_id = segment.null_value_id();
  resolve_compressed_vector_type(*segment.attribute_vector(), [&](const auto& attribute_vector) {
    for (const auto& value_id : attribute_vector) {
      if (value_id < null_value_id) {
        ++value_distribution_vector[value_id].second;
      }
    }
  });

  std::cerr << std::format("Dict phase: {} us\n", (std::chrono::duration<double>{std::chrono::steady_clock::now() - start}.count() * 1'000'000));
}

template <typename T>
void process_any_unspecialized(AbstractSegment& segment, ValueDistributionVector<T>& value_distribution_vector, StringHeap& string_heap, const HistogramDomain<T>& domain) {
  auto value_distribution_map = ValueDistributionMap<T>{};  
  // value_distribution_map.reserve(segment.size() * 2);
  value_distribution_map.reserve(segment.size());

  segment_iterate<T>(segment, [&](const auto& iterator_value) {
    if (iterator_value.is_null()) {
      return;
    }

    if constexpr (std::is_same_v<T, pmr_string>) {
      const auto current_string = string_heap.add_string(domain.string_to_domain(iterator_value.value()));
      ++value_distribution_map[current_string];
    } else {
      ++value_distribution_map[iterator_value.value()];
    }
  });

  value_distribution_vector.insert(value_distribution_vector.begin(), value_distribution_map.cbegin(), value_distribution_map.cend());
}

template <typename T>
ValueDistributionVector<T> add_segment_to_value_distribution2(const size_t max_parallel_level, const size_t level, std::vector<std::unique_ptr<StringHeap>>& string_heaps,
                                                              auto segment_iterator_begin, auto segment_iterator_end,
                                                              const HistogramDomain<T>& domain) {
  // std::cerr << "level " << level << std::endl;
  DebugAssert(segment_iterator_begin != segment_iterator_end, "Wrong call");
  if (std::distance(segment_iterator_begin, segment_iterator_end) == 1) {
    const auto [chunk_id, segment] = *segment_iterator_begin;

    string_heaps[chunk_id] = std::make_unique<StringHeap>();
    auto& string_heap = *string_heaps[chunk_id];

    auto value_distribution_vector = ValueDistributionVector<T>{};

    // We specialize for dictionaries, as they are used for string columns (which are expensive in histogram creation).
    // We can string_view strings. We could do the same for Unencoded, but this is currently not implemented as they are
    // barely used for strings.
    resolve_segment_type<T>(*segment, [&, segment=segment](const auto& typed_segment) {
      using SegmentType = std::decay_t<decltype(typed_segment)>;
      if constexpr (std::is_same_v<SegmentType, ReferenceSegment>) {
        Fail("Unexpected reference segment.");
      } else if constexpr (std::is_same_v<SegmentType, DictionarySegment<T>>) {
        process_dictionary_segment<T>(typed_segment, *typed_segment.dictionary(), value_distribution_vector, string_heap, domain);
      } else if constexpr (std::is_same_v<SegmentType, FixedStringDictionarySegment<T>>) {
        process_dictionary_segment<T>(typed_segment, *typed_segment.fixed_string_dictionary(), value_distribution_vector, string_heap, domain);
      } else {
        process_any_unspecialized<T>(*segment, value_distribution_vector, string_heap, domain);  
      }
    });

    boost::sort::pdqsort(value_distribution_vector.begin(), value_distribution_vector.end(), [&](const auto& lhs, const auto& rhs) {
      return lhs.first < rhs.first;
    });

    return value_distribution_vector;
  }

  auto left = ValueDistributionVector<T>{};
  auto right = ValueDistributionVector<T>{};

  auto middle = segment_iterator_begin + (std::distance(segment_iterator_begin, segment_iterator_end) / 2);

  auto left_task = [&]() {
    left = add_segment_to_value_distribution2(max_parallel_level, level + 1, string_heaps, segment_iterator_begin, middle, domain);
  };
  auto right_task = [&]() {
    right = add_segment_to_value_distribution2(max_parallel_level, level + 1, string_heaps, middle, segment_iterator_end, domain);
  };

  if (level < max_parallel_level) {
    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{std::make_shared<JobTask>(left_task),
                                                            std::make_shared<JobTask>(right_task)};
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  } else {
    left_task();
    right_task();
  }

  const auto start = std::chrono::steady_clock::now();
  auto result = ValueDistributionVector<T>{};
  result.reserve(left.size() + right.size());  

  auto left_iter = left.begin();
  auto right_iter = right.begin();


  while (left_iter != left.end() || right_iter != right.end()) {
    if (left_iter == left.end()) {
      result.emplace_back(*right_iter);
      ++right_iter;
      continue;
    } else if (right_iter == right.end()) {
      result.emplace_back(*left_iter);
      ++left_iter;
      continue;
    }

    if (left_iter->first == right_iter->first) {
      result.emplace_back(left_iter->first, left_iter->second + right_iter->second);
      ++left_iter;
      ++right_iter;
    } else if (left_iter->first < right_iter->first) {
      result.emplace_back(*left_iter);
      ++left_iter;
    } else {
      result.emplace_back(*right_iter);
      ++right_iter;
    }
  }

  std::cerr << std::format("Merging: {} us\n", (std::chrono::duration<double>{std::chrono::steady_clock::now() - start}.count() * 1'000'000));

  DebugAssert(std::is_sorted(result.begin(), result.end()), "Not sorted.");
  return result;
}

template <typename T>
std::pair<std::vector<std::unique_ptr<StringHeap>>,
          ValueDistributionVector<T>> value_distribution_from_column(const Table& table,
                                                                     const ColumnID column_id,
                                                                     const HistogramDomain<T>& domain) {
  // auto value_distribution_map = ValueDistributionMap<T>{};
  const auto chunk_count = table.chunk_count();
  auto segments_to_process = std::vector<std::pair<ChunkID, std::shared_ptr<AbstractSegment>>>{};
  segments_to_process.reserve(std::min(chunk_count, MIN_CHUNK_COUNT_TO_INCLUDE));

  // In average, we sample every 2.5th segment. Previous analyses of Hyrise histograms showed that the accuracy of
  // sample histograms quickly deteriogates with sampling rates below 33 %. We thus choose a safer rate of ~40%.
  auto random_engine = std::ranlux24_base{17};  // Fast random engine. Sufficient for our case.
  auto skip_lengths = std::uniform_int_distribution<>{0, 4};

  auto current_chunk_id = ChunkID{0};
  while (current_chunk_id < chunk_count) {
    const auto chunk = table.get_chunk(current_chunk_id);
    if (!chunk) {
      continue;
    }

    segments_to_process.emplace_back(current_chunk_id, chunk->get_segment(column_id));

    if (current_chunk_id < MIN_CHUNK_COUNT_TO_INCLUDE / 2 || current_chunk_id >= (chunk_count - (MIN_CHUNK_COUNT_TO_INCLUDE / 2))) {
      ++current_chunk_id;
      continue;
    }

    current_chunk_id += (skip_lengths(random_engine) % 4) + 1;
  }

  auto string_heaps = std::vector<std::unique_ptr<StringHeap>>{};
  string_heaps.resize(chunk_count);

  auto result = ValueDistributionVector<T>{};
  if (chunk_count > 0) {
    const auto parallel_levels_cpus = static_cast<size_t>(std::ceil(std::log2(Hyrise::get().topology.num_cpus()))) + 1;
    result = add_segment_to_value_distribution2<T>(parallel_levels_cpus, 0, string_heaps, segments_to_process.begin(), segments_to_process.end(), domain);
  }

  return {std::move(string_heaps), std::move(result)};
}

}  // namespace

namespace hyrise {

template <typename T>
EqualDistinctCountHistogram<T>::EqualDistinctCountHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                                                            std::vector<HistogramCountType>&& bin_heights,
                                                            const HistogramCountType distinct_count_per_bin,
                                                            const BinID bin_count_with_extra_value,
                                                            const HistogramDomain<T>& domain)
    : AbstractHistogram<T>(domain),
      _bin_minima(std::move(bin_minima)),
      _bin_maxima(std::move(bin_maxima)),
      _bin_heights(std::move(bin_heights)),
      _distinct_count_per_bin(distinct_count_per_bin),
      _bin_count_with_extra_value(bin_count_with_extra_value) {
  Assert(_bin_minima.size() == _bin_maxima.size(), "Must have the same number of lower as upper bin edges.");
  Assert(_bin_minima.size() == _bin_heights.size(), "Must have the same number of edges and heights.");
  Assert(_distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  Assert(_bin_count_with_extra_value < _bin_minima.size(), "Cannot have more bins with extra value than bins.");

  AbstractHistogram<T>::_assert_bin_validity();

  _total_count = std::accumulate(_bin_heights.cbegin(), _bin_heights.cend(), HistogramCountType{0});
  _total_distinct_count = _distinct_count_per_bin * static_cast<HistogramCountType>(_bin_heights.size()) +
                          static_cast<HistogramCountType>(_bin_count_with_extra_value);
}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::from_column(
    const Table& table, const ColumnID column_id, const BinID max_bin_count, const HistogramDomain<T>& domain) {
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero ");

  const auto& [string_heaps, value_distribution] = value_distribution_from_column(table, column_id, domain);

  if (value_distribution.empty()) {
    return nullptr;
  }

  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count =
      value_distribution.size() < max_bin_count ? static_cast<BinID>(value_distribution.size()) : max_bin_count;

  // Split values evenly among bins.
  const auto distinct_count_per_bin = static_cast<size_t>(value_distribution.size() / bin_count);
  const BinID bin_count_with_extra_value = value_distribution.size() % bin_count;

  auto bin_minima = std::vector<T>(bin_count);
  auto bin_maxima = std::vector<T>(bin_count);
  auto bin_heights = std::vector<HistogramCountType>(bin_count);

  // `min_value_idx` and `max_value_idx` are indices into the sorted vector `value_distribution` describing which range
  // of distinct values goes into a bin.
  auto min_value_idx = BinID{0};
  for (BinID bin_idx = 0; bin_idx < bin_count; bin_idx++) {
    auto max_value_idx = min_value_idx + distinct_count_per_bin - 1;
    if (bin_idx < bin_count_with_extra_value) {
      max_value_idx++;
    }

    if constexpr (std::is_same_v<T, pmr_string>) {
      bin_minima[bin_idx] = T{value_distribution[min_value_idx].first.data()};
      bin_maxima[bin_idx] = T{value_distribution[max_value_idx].first.data()};  
    } else {
      bin_minima[bin_idx] = T{value_distribution[min_value_idx].first};
      bin_maxima[bin_idx] = T{value_distribution[max_value_idx].first};
    }

    

    bin_heights[bin_idx] =
        std::accumulate(value_distribution.cbegin() + min_value_idx, value_distribution.cbegin() + max_value_idx + 1,
                        HistogramCountType{0},
                        [](HistogramCountType bin_height, const auto& value_and_count) {
                          return bin_height + static_cast<HistogramCountType>(value_and_count.second);
                        });

    min_value_idx = max_value_idx + 1;
  }

  // auto ss = std::stringstream{};
  // ss << "\nminima: ";
  // for (auto bin_id = BinID{0}; bin_id < bin_count; ++bin_id) {
  //   ss << bin_minima[bin_id];
  // }
  // ss << "\n\nmaxima: ";
  // for (auto bin_id = BinID{0}; bin_id < bin_count; ++bin_id) {
  //   ss << bin_maxima[bin_id];
  // }
  // ss << "\n\n";
  // std::cerr << ss.str();

  return std::make_shared<EqualDistinctCountHistogram<T>>(
      std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
      static_cast<HistogramCountType>(distinct_count_per_bin), bin_count_with_extra_value);
}

template <typename T>
std::string EqualDistinctCountHistogram<T>::name() const {
  return "EqualDistinctCount";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> EqualDistinctCountHistogram<T>::clone() const {
  // The new histogram needs a copy of the data
  auto bin_minima_copy = _bin_minima;
  auto bin_maxima_copy = _bin_maxima;
  auto bin_heights_copy = _bin_heights;

  return std::make_shared<EqualDistinctCountHistogram<T>>(std::move(bin_minima_copy), std::move(bin_maxima_copy),
                                                          std::move(bin_heights_copy), _distinct_count_per_bin,
                                                          _bin_count_with_extra_value);
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::bin_count() const {
  return _bin_heights.size();
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::bin_for_value(const T& value) const {
  const auto iter = std::lower_bound(_bin_maxima.cbegin(), _bin_maxima.cend(), value);
  const auto index = static_cast<BinID>(std::distance(_bin_maxima.cbegin(), iter));

  if (iter == _bin_maxima.cend() || value < bin_minimum(index) || value > bin_maximum(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::next_bin_for_value(const T& value) const {
  const auto it = std::upper_bound(_bin_maxima.cbegin(), _bin_maxima.cend(), value);

  if (it == _bin_maxima.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_maxima.cbegin(), it));
}

template <typename T>
const T& EqualDistinctCountHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index < _bin_minima.size(), "Index is not a valid bin.");
  return _bin_minima[index];
}

template <typename T>
const T& EqualDistinctCountHistogram<T>::bin_maximum(const BinID index) const {
  DebugAssert(index < _bin_maxima.size(), "Index is not a valid bin.");
  return _bin_maxima[index];
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::bin_height(const BinID index) const {
  DebugAssert(index < _bin_heights.size(), "Index is not a valid bin.");
  return _bin_heights[index];
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::bin_distinct_count(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return HistogramCountType{_distinct_count_per_bin + (index < _bin_count_with_extra_value ? 1 : 0)};
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::total_distinct_count() const {
  return _total_distinct_count;
}

template <typename T>
uint16_t EqualDistinctCountHistogram<T>::determine_bin_count(size_t table_row_count) {
  // Determine bin count, within mostly arbitrarily chosen bounds: 8 (for tables with <= 16000 rows) up to 100 bins (for
  // tables with >= 256 000 rows) are created.
  // If the table does not have a high number of distinct values, the EqualDistinctCountHistogram automatically uses
  // fewer bins.
  return static_cast<uint16_t>(std::min<size_t>(128, std::max<size_t>(8, table_row_count / 2'000)));
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualDistinctCountHistogram);

}  // namespace hyrise
