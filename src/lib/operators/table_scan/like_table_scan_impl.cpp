#include "like_table_scan_impl.hpp"

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

LikeTableScanImpl::LikeTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID left_column_id,
                                     const PredicateCondition predicate_condition, const std::string& pattern)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, predicate_condition},
      _matcher{pattern},
      _invert_results(predicate_condition == PredicateCondition::NotLike) {}

std::string LikeTableScanImpl::description() const { return "LikeScan"; }

void LikeTableScanImpl::handle_segment(const BaseValueSegment& base_segment,
                                       std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;
  auto& left_segment = static_cast<const ValueSegment<std::string>&>(base_segment);
  auto left_iterable = ValueSegmentIterable<std::string>{left_segment};

  _scan_iterable(left_iterable, chunk_id, matches_out, mapped_chunk_offsets.get());
}

void LikeTableScanImpl::handle_segment(const BaseEncodedSegment& base_segment,
                                       std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  resolve_encoded_segment_type<std::string>(base_segment, [&](const auto& typed_segment) {
    auto left_iterable = create_iterable_from_segment(typed_segment);
    _scan_iterable(left_iterable, chunk_id, matches_out, mapped_chunk_offsets.get());
  });
}

void LikeTableScanImpl::handle_segment(const BaseDictionarySegment& base_segment,
                                       std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  std::pair<size_t, std::vector<bool>> result;

  if (base_segment.encoding_type() == EncodingType::Dictionary) {
    const auto& left_segment = static_cast<const DictionarySegment<std::string>&>(base_segment);
    result = _find_matches_in_dictionary(*left_segment.dictionary());
  } else {
    const auto& left_segment = static_cast<const FixedStringDictionarySegment<std::string>&>(base_segment);
    result = _find_matches_in_dictionary(*left_segment.dictionary());
  }

  const auto& match_count = result.first;
  const auto& dictionary_matches = result.second;

  auto attribute_vector_iterable = create_iterable_from_attribute_vector(base_segment);

  // LIKE matches all rows
  if (match_count == dictionary_matches.size()) {
    attribute_vector_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      static const auto always_true = [](const auto&) { return true; };
      this->_unary_scan(always_true, left_it, left_end, chunk_id, matches_out);
    });

    return;
  }

  // LIKE matches no rows
  if (match_count == 0u) {
    return;
  }

  const auto dictionary_lookup = [&dictionary_matches](const ValueID& value) { return dictionary_matches[value]; };

  attribute_vector_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    this->_unary_scan(dictionary_lookup, left_it, left_end, chunk_id, matches_out);
  });
}

template <typename Iterable>
void LikeTableScanImpl::_scan_iterable(const Iterable& iterable, const ChunkID chunk_id, PosList& matches_out,
                                       const ChunkOffsetsList* const mapped_chunk_offsets) {
  _matcher.resolve(_invert_results, [&](const auto& matcher) {
    iterable.with_iterators(mapped_chunk_offsets, [&](auto left_it, auto left_end) {
      this->_unary_scan(matcher, left_it, left_end, chunk_id, matches_out);
    });
  });
}

std::pair<size_t, std::vector<bool>> LikeTableScanImpl::_find_matches_in_dictionary(
    const pmr_vector<std::string>& dictionary) {
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
