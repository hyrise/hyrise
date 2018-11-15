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
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

namespace opossum {

ColumnLikeTableScanImpl::ColumnLikeTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                                 const PredicateCondition predicate_condition,
                                                 const std::string& pattern)
    : AbstractSingleColumnTableScanImpl{in_table, column_id, predicate_condition},
      _matcher{pattern},
      _invert_results(predicate_condition == PredicateCondition::NotLike) {}

std::string ColumnLikeTableScanImpl::description() const { return "LikeScan"; }

void ColumnLikeTableScanImpl::_scan_non_reference_segment(const BaseSegment& segment, const ChunkID chunk_id,
                                                          PosList& matches,
                                                          const std::shared_ptr<const PosList>& position_filter) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    _scan_segment(typed_segment, chunk_id, matches, position_filter);
  });
}

void ColumnLikeTableScanImpl::_scan_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
                                            const std::shared_ptr<const PosList>& position_filter) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    using Type = typename decltype(type)::type;
    if constexpr (std::is_same_v<decltype(typed_segment), const ReferenceSegment&>) {
      Fail("Expected ReferenceSegments to be handled before calling this method");
    } else if constexpr (!std::is_same_v<Type, std::string>) {
      Fail("Can only handle strings");
    } else {
      _matcher.resolve(_invert_results, [&](const auto& resolved_matcher) {
        const auto functor = [&](const auto& iterator_value) { return resolved_matcher(iterator_value.value()); };

        auto iterable = create_iterable_from_segment(typed_segment);
        iterable.with_iterators(position_filter, [&](auto it, auto end) {
          _scan_with_iterators<true>(functor, it, end, chunk_id, matches);
        });
      });
    }
  });
}

void ColumnLikeTableScanImpl::_scan_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id,
                                            PosList& matches,
                                            const std::shared_ptr<const PosList>& position_filter) const {
  std::pair<size_t, std::vector<bool>> result;

  if (segment.encoding_type() == EncodingType::Dictionary) {
    const auto& typed_segment = static_cast<const DictionarySegment<std::string>&>(segment);
    result = _find_matches_in_dictionary(*typed_segment.dictionary());
  } else {
    const auto& typed_segment = static_cast<const FixedStringDictionarySegment<std::string>&>(segment);
    result = _find_matches_in_dictionary(*typed_segment.dictionary());
  }

  const auto& match_count = result.first;
  const auto& dictionary_matches = result.second;

  auto attribute_vector_iterable = create_iterable_from_attribute_vector(segment);

  // LIKE matches all rows
  if (match_count == dictionary_matches.size()) {
    attribute_vector_iterable.with_iterators(position_filter, [&](auto it, auto end) {
      static const auto always_true = [](const auto&) { return true; };
      _scan_with_iterators<false>(always_true, it, end, chunk_id, matches);
    });

    return;
  }

  // LIKE matches no rows
  if (match_count == 0u) {
    return;
  }

  const auto dictionary_lookup = [&dictionary_matches](const auto& iterator_value) {
    return dictionary_matches[iterator_value.value()];
  };

  attribute_vector_iterable.with_iterators(position_filter, [&](auto it, auto end) {
    _scan_with_iterators<true>(dictionary_lookup, it, end, chunk_id, matches);
  });
}

std::pair<size_t, std::vector<bool>> ColumnLikeTableScanImpl::_find_matches_in_dictionary(
    const pmr_vector<std::string>& dictionary) const {
  auto result = std::pair<size_t, std::vector<bool>>{};

  auto& count = result.first;
  auto& dictionary_matches = result.second;

  count = 0u;
  dictionary_matches.reserve(dictionary.size());

  _matcher.resolve(_invert_results, [&](const auto& matcher) {
    for (const auto& value : dictionary) {
      const auto result = matcher(value);
      count += static_cast<size_t>(result);
      dictionary_matches.push_back(result);
    }
  });

  return result;
}

}  // namespace opossum
