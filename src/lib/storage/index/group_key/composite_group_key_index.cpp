#include "composite_group_key_index.hpp"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <iterator>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "storage/base_dictionary_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_utils.hpp"
#include "utils/assert.hpp"
#include "variable_length_key_proxy.hpp"

namespace opossum {

size_t CompositeGroupKeyIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                                           uint32_t value_bytes) {
  return ((row_count + distinct_count) * sizeof(ChunkOffset) + distinct_count * value_bytes);
}

CompositeGroupKeyIndex::CompositeGroupKeyIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index)
    : AbstractIndex{get_index_type_of<CompositeGroupKeyIndex>()} {
  Assert(!segments_to_index.empty(), "CompositeGroupKeyIndex requires at least one segment to be indexed.");

  if (HYRISE_DEBUG) {
    auto first_size = segments_to_index.front()->size();
    [[maybe_unused]] auto all_segments_have_same_size =
        std::all_of(segments_to_index.cbegin(), segments_to_index.cend(),
                    [first_size](const auto& segment) { return segment->size() == first_size; });

    DebugAssert(all_segments_have_same_size,
                "CompositeGroupKey requires same length of all segments that should be indexed.");
  }

  // cast and check segments
  _indexed_segments.reserve(segments_to_index.size());
  for (const auto& segment : segments_to_index) {
    auto dict_segment = std::dynamic_pointer_cast<const BaseDictionarySegment>(segment);
    Assert(static_cast<bool>(dict_segment), "CompositeGroupKeyIndex only works with dictionary segments.");
    Assert(dict_segment->compressed_vector_type(),
           "Expected DictionarySegment to use vector compression for attribute vector");
    Assert(is_fixed_size_byte_aligned(*dict_segment->compressed_vector_type()),
           "CompositeGroupKeyIndex only works with fixed-size byte-aligned compressed attribute vectors.");
    _indexed_segments.emplace_back(dict_segment);
  }

  // retrieve amount of memory consumed by each concatenated key
  auto bytes_per_key = std::accumulate(
      _indexed_segments.begin(), _indexed_segments.end(), CompositeKeyLength{0u},
      [](auto key_length, const auto& segment) {
        return key_length + byte_width_for_fixed_size_byte_aligned_type(*segment->compressed_vector_type());
      });

  // create concatenated keys and save their positions
  // at this point duplicated keys may be created, they will be handled later
  auto segment_size = _indexed_segments.front()->size();
  auto keys = std::vector<VariableLengthKey>(segment_size);
  _position_list.resize(segment_size);

  auto attribute_vector_widths_and_decompressors = [&]() {
    auto decompressors =
        std::vector<std::pair<size_t, std::unique_ptr<BaseVectorDecompressor>>>(_indexed_segments.size());

    std::transform(
        _indexed_segments.cbegin(), _indexed_segments.cend(), decompressors.begin(), [](const auto& segment) {
          const auto byte_width = byte_width_for_fixed_size_byte_aligned_type(*segment->compressed_vector_type());
          auto decompressor = segment->attribute_vector()->create_base_decompressor();
          return std::make_pair(byte_width, std::move(decompressor));
        });

    return decompressors;
  }();

  for (ChunkOffset chunk_offset = 0; chunk_offset < static_cast<ChunkOffset>(segment_size); ++chunk_offset) {
    auto concatenated_key = VariableLengthKey(bytes_per_key);
    for (const auto& [byte_width, decompressor] : attribute_vector_widths_and_decompressors) {
      concatenated_key.shift_and_set(decompressor->get(chunk_offset), static_cast<uint8_t>(byte_width * CHAR_BIT));
    }
    keys[chunk_offset] = std::move(concatenated_key);
    _position_list[chunk_offset] = chunk_offset;
  }

  // sort keys and their positions
  std::sort(_position_list.begin(), _position_list.end(),
            [&keys](auto left, auto right) { return keys[left] < keys[right]; });

  _keys = VariableLengthKeyStore(static_cast<ChunkOffset>(segment_size), bytes_per_key);
  for (ChunkOffset chunk_offset = 0; chunk_offset < static_cast<ChunkOffset>(segment_size); ++chunk_offset) {
    _keys[chunk_offset] = keys[_position_list[chunk_offset]];
  }

  // create offsets to unique keys
  _key_offsets.reserve(segment_size);
  _key_offsets.emplace_back(0);
  for (ChunkOffset chunk_offset = 1; chunk_offset < static_cast<ChunkOffset>(segment_size); ++chunk_offset) {
    if (_keys[chunk_offset] != _keys[chunk_offset - 1]) _key_offsets.emplace_back(chunk_offset);
  }
  _key_offsets.shrink_to_fit();

  // remove duplicated keys
  auto unique_keys_end = std::unique(_keys.begin(), _keys.end());
  _keys.erase(unique_keys_end, _keys.end());
  _keys.shrink_to_fit();
}

AbstractIndex::Iterator CompositeGroupKeyIndex::_cbegin() const { return _position_list.cbegin(); }

AbstractIndex::Iterator CompositeGroupKeyIndex::_cend() const { return _position_list.cend(); }

AbstractIndex::Iterator CompositeGroupKeyIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  auto composite_key = _create_composite_key(values, false);
  return _get_position_iterator_for_key(composite_key);
}

AbstractIndex::Iterator CompositeGroupKeyIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  auto composite_key = _create_composite_key(values, true);
  return _get_position_iterator_for_key(composite_key);
}

VariableLengthKey CompositeGroupKeyIndex::_create_composite_key(const std::vector<AllTypeVariant>& values,
                                                                bool is_upper_bound) const {
  auto result = VariableLengthKey(_keys.key_size());

  // retrieve the partial keys for every value except for the last one and append them into one partial-key
  for (auto column_id = ColumnID{0}; column_id < values.size() - 1; ++column_id) {
    Assert(!variant_is_null(values[column_id]), "CompositeGroupKeyIndex doesn't support NULL handling yet.");
    auto partial_key = _indexed_segments[column_id]->lower_bound(values[column_id]);
    auto bits_of_partial_key =
        byte_width_for_fixed_size_byte_aligned_type(*_indexed_segments[column_id]->compressed_vector_type()) * CHAR_BIT;
    result.shift_and_set(partial_key, static_cast<uint8_t>(bits_of_partial_key));
  }

  // retrieve the partial key for the last value (depending on whether we have a lower- or upper-bound-query)
  // and append it to the previously created partial key to obtain the key containing all provided values
  const auto& segment_for_last_value = _indexed_segments[values.size() - 1];
  auto&& partial_key = is_upper_bound ? segment_for_last_value->upper_bound(values.back())
                                      : segment_for_last_value->lower_bound(values.back());
  auto bits_of_partial_key =
      byte_width_for_fixed_size_byte_aligned_type(*segment_for_last_value->compressed_vector_type()) * CHAR_BIT;
  result.shift_and_set(partial_key, static_cast<uint8_t>(bits_of_partial_key));

  // fill empty space of key with zeros if less values than segments were provided
  auto empty_bits = std::accumulate(
      _indexed_segments.cbegin() + values.size(), _indexed_segments.cend(), static_cast<uint8_t>(0u),
      [](auto value, auto segment) {
        return value + byte_width_for_fixed_size_byte_aligned_type(*segment->compressed_vector_type()) * CHAR_BIT;
      });
  result <<= empty_bits;

  return result;
}

AbstractIndex::Iterator CompositeGroupKeyIndex::_get_position_iterator_for_key(const VariableLengthKey& key) const {
  // get an iterator pointing to the search-key in the keystore
  // (use always lower_bound() since the search method is already handled within creation of composite key)
  auto key_it = std::lower_bound(_keys.cbegin(), _keys.cend(), key);
  if (key_it == _keys.cend()) return _position_list.cend();

  // get the start position in the position-vector, ie the offset, by getting the offset_iterator for the key
  // (which is at the same position as the iterator for the key in the keystore)
  auto offset_it = _key_offsets.cbegin();
  std::advance(offset_it, std::distance(_keys.cbegin(), key_it));

  // get an iterator pointing to that start position
  auto position_it = _position_list.cbegin();
  std::advance(position_it, *offset_it);

  return position_it;
}

std::vector<std::shared_ptr<const BaseSegment>> CompositeGroupKeyIndex::_get_indexed_segments() const {
  auto result = std::vector<std::shared_ptr<const BaseSegment>>();
  result.reserve(_indexed_segments.size());
  for (auto&& indexed_segment : _indexed_segments) {
    result.emplace_back(indexed_segment);
  }
  return result;
}

size_t CompositeGroupKeyIndex::_memory_consumption() const {
  size_t byte_count = _keys.size() * _keys.key_size();
  byte_count += _key_offsets.size() * sizeof(ChunkOffset);
  byte_count += _position_list.size() * sizeof(ChunkOffset);
  return byte_count;
}

}  // namespace opossum
