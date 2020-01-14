#include "column_like_table_scan_impl.hpp"

#include <algorithm>
#include <array>
#include <map>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "storage/create_iterable_from_segment.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

namespace opossum {

ColumnLikeTableScanImpl::ColumnLikeTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                                 const PredicateCondition init_predicate_condition,
                                                 const pmr_string& pattern)
    : AbstractDereferencedColumnTableScanImpl{in_table, column_id, init_predicate_condition},
      _matcher{pattern},
      _invert_results(predicate_condition == PredicateCondition::NotLike) {}

std::string ColumnLikeTableScanImpl::description() const { return "ColumnLike"; }

void ColumnLikeTableScanImpl::_scan_non_reference_segment(const BaseSegment& segment, const ChunkID chunk_id,
                                                          PosList& matches,
                                                          const std::shared_ptr<const PosList>& position_filter) const {
  // For dictionary segments where the number of unique values is not higher than the number of (potentially filtered)
  // input rows, use an optimized implementation.
  if (const auto* dictionary_segment = dynamic_cast<const BaseDictionarySegment*>(&segment);
      dictionary_segment &&
      (!position_filter || dictionary_segment->unique_values_count() <= position_filter->size())) {
    _scan_dictionary_segment(*dictionary_segment, chunk_id, matches, position_filter);
  } else {
    _scan_generic_segment(segment, chunk_id, matches, position_filter);
  }
}

void ColumnLikeTableScanImpl::_scan_generic_segment(const BaseSegment& segment, const ChunkID chunk_id,
                                                    PosList& matches,
                                                    const std::shared_ptr<const PosList>& position_filter) const {
  segment_with_iterators_filtered(segment, position_filter, [&](auto it, [[maybe_unused]] const auto end) {
    // Don't instantiate this for ReferenceSegments to save compile time as ReferenceSegments are handled
    // via position_filter
    if constexpr (!is_reference_segment_iterable_v<typename decltype(it)::IterableType>) {
      using ColumnDataType = typename decltype(it)::ValueType;

      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        _matcher.resolve(_invert_results, [&](const auto& resolved_matcher) {
          const auto functor = [&](const auto& position) { return resolved_matcher(position.value()); };
          _scan_with_iterators<true>(functor, it, end, chunk_id, matches);
        });
      } else {
        Fail("Can only handle strings");
      }
    } else {
      Fail("ReferenceSegments have their own code paths and should be handled there");
    }
  });
}

void ColumnLikeTableScanImpl::_scan_dictionary_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id,
                                                       PosList& matches,
                                                       const std::shared_ptr<const PosList>& position_filter) const {
  // First, build a bitmap containing 1s/0s for matching/non-matching dictionary values. Second, iterate over the
  // attribute vector and check against the bitmap. If too many input rows have already been removed (are not part of
  // position_filter), this optimization is detrimental. See caller for that case.
  std::pair<size_t, std::vector<bool>> result;

  if (segment.encoding_type() == EncodingType::Dictionary) {
    const auto& typed_segment = static_cast<const DictionarySegment<pmr_string>&>(segment);
    result = _find_matches_in_dictionary(*typed_segment.dictionary());
  } else {
    const auto& typed_segment = static_cast<const FixedStringDictionarySegment<pmr_string>&>(segment);
    result = _find_matches_in_dictionary(*typed_segment.fixed_string_dictionary());
  }

  const auto& match_count = result.first;
  const auto& dictionary_matches = result.second;

  auto attribute_vector_iterable = create_iterable_from_attribute_vector(segment);

  // LIKE matches all rows, but we still need to check for NULL
  if (match_count == dictionary_matches.size()) {
    attribute_vector_iterable.with_iterators(position_filter, [&](auto it, auto end) {
      static const auto always_true = [](const auto&) { return true; };
      _scan_with_iterators<true>(always_true, it, end, chunk_id, matches);
    });

    return;
  }

  // LIKE matches no rows
  if (match_count == 0u) {
    return;
  }

  const auto dictionary_lookup = [&dictionary_matches](const auto& position) {
    return dictionary_matches[position.value()];
  };

  attribute_vector_iterable.with_iterators(position_filter, [&](auto it, auto end) {
    _scan_with_iterators<true>(dictionary_lookup, it, end, chunk_id, matches);
  });
}

template <typename D>
std::pair<size_t, std::vector<bool>> ColumnLikeTableScanImpl::_find_matches_in_dictionary(const D& dictionary) const {
  auto result = std::pair<size_t, std::vector<bool>>{};

  auto& count = result.first;
  auto& dictionary_matches = result.second;

  count = 0u;
  dictionary_matches.reserve(dictionary.size());

  _matcher.resolve(_invert_results, [&](const auto& matcher) {
#ifdef __clang__
// For the loop through the dictionary, we want to use const auto& for DictionaySegments. However,
// FixedStringVector iterators return an std::string_view value. Thus, we disable clang's -Wrange-loop-analysis
// error about a potential copy for the loop value.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wrange-loop-analysis"
#endif
    for (const auto& value : dictionary) {
      const auto matches = matcher(value);
      count += static_cast<size_t>(matches);
      dictionary_matches.push_back(matches);
    }

#ifdef __clang__
#pragma clang diagnostic pop
#endif
  });

  return result;
}

}  // namespace opossum
