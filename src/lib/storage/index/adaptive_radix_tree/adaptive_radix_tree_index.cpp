#include "adaptive_radix_tree_index.hpp"

#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "adaptive_radix_tree_nodes.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/index/base_index.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

size_t AdaptiveRadixTreeIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                                           uint32_t value_bytes) {
  Fail("AdaptiveRadixTreeIndex::estimate_memory_consumption() is not implemented yet");
}

AdaptiveRadixTreeIndex::AdaptiveRadixTreeIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index)
    : BaseIndex{get_index_type_of<AdaptiveRadixTreeIndex>()},
      _indexed_segment(std::dynamic_pointer_cast<const BaseDictionarySegment>(segments_to_index.front())) {
  Assert(static_cast<bool>(_indexed_segment), "AdaptiveRadixTree only works with dictionary segments for now");
  Assert((segments_to_index.size() == 1), "AdaptiveRadixTree only works with a single segment");

  // For each value ID in the attribute vector, create a pair consisting of a BinaryComparable of
  // this value ID and its ChunkOffset (needed for bulk-inserting).
  std::vector<std::pair<BinaryComparable, ChunkOffset>> pairs_to_insert;
  pairs_to_insert.reserve(_indexed_segment->attribute_vector()->size());

  resolve_compressed_vector_type(*_indexed_segment->attribute_vector(), [&](const auto& attribute_vector) {
    auto chunk_offset = ChunkOffset{0u};
    auto value_id_it = attribute_vector.cbegin();
    for (; value_id_it != attribute_vector.cend(); ++value_id_it, ++chunk_offset) {
      pairs_to_insert.emplace_back(BinaryComparable(ValueID{*value_id_it}), chunk_offset);
    }
  });

  _root = _bulk_insert(pairs_to_insert);
}

BaseIndex::Iterator AdaptiveRadixTreeIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  assert(values.size() == 1);
  ValueID value_id = _indexed_segment->lower_bound(values[0]);
  if (value_id == INVALID_VALUE_ID) {
    return _chunk_offsets.end();
  }
  return _root->lower_bound(BinaryComparable(value_id), 0);
}

BaseIndex::Iterator AdaptiveRadixTreeIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  assert(values.size() == 1);
  ValueID value_id = _indexed_segment->upper_bound(values[0]);
  if (value_id == INVALID_VALUE_ID) {
    return _chunk_offsets.end();
  } else {
    return _root->lower_bound(BinaryComparable(value_id), 0);
  }
}

BaseIndex::Iterator AdaptiveRadixTreeIndex::_cbegin() const { return _chunk_offsets.cbegin(); }

BaseIndex::Iterator AdaptiveRadixTreeIndex::_cend() const { return _chunk_offsets.cend(); }

std::shared_ptr<ARTNode> AdaptiveRadixTreeIndex::_bulk_insert(
    const std::vector<std::pair<BinaryComparable, ChunkOffset>>& values) {
  DebugAssert(!(values.empty()), "Index on empty segment is not defined");
  _chunk_offsets.reserve(values.size());
  auto begin = _chunk_offsets.cbegin();
  return _bulk_insert(values, static_cast<size_t>(0u), begin);
}

std::shared_ptr<ARTNode> AdaptiveRadixTreeIndex::_bulk_insert(
    const std::vector<std::pair<BinaryComparable, ChunkOffset>>& values, size_t depth, BaseIndex::Iterator& it) {
  // This is the anchor of the recursion: if all values have the same key, create a leaf.
  if (std::all_of(values.begin(), values.end(), [&values](const std::pair<BinaryComparable, ChunkOffset>& pair) {
        return values.front().first == pair.first;
      })) {
    // copy the Iterator in the _chunk_offsets - vector --> this is the lower_bound of the leaf
    auto lower = it;
    // insert the ChunkOffsets into the vector and push the Iterator further
    auto old_capacity = _chunk_offsets.capacity();
    for (const auto& pair : values) {
      _chunk_offsets.emplace_back(pair.second);
    }
    std::advance(it, values.size());
    auto new_capacity = _chunk_offsets.capacity();
    // we are not allowed to change the size of the vector as it would invalidate all our Iterators
    Assert(old_capacity == new_capacity, "_chunk_offsets capacity changes, all Iterators are invalidated");

    // "it" points to the position after the last inserted ChunkOffset --> this is the upper_bound of the leave
    auto upper = it;
    return std::make_shared<Leaf>(lower, upper);
  }

  // radix-partition on the depths-byte into 256 partitions
  std::array<std::vector<std::pair<BinaryComparable, ChunkOffset>>, std::numeric_limits<uint8_t>::max() + 1> partitions;
  for (const auto& pair : values) {
    partitions[pair.first[depth]].emplace_back(pair);
  }

  // call recursively for each non-empty partition and gather the children
  std::vector<std::pair<uint8_t, std::shared_ptr<ARTNode>>> children;

  for (uint16_t partition_id = 0; partition_id < partitions.size(); ++partition_id) {
    if (!partitions[partition_id].empty()) {
      auto child =
          std::make_pair(static_cast<uint8_t>(partition_id), _bulk_insert(partitions[partition_id], depth + 1, it));
      children.emplace_back(std::move(child));
    }
  }
  // finally create the appropriate ARTNode according to the size of the children
  if (children.size() <= 4) {
    return std::make_shared<ARTNode4>(children);
  } else if (children.size() <= 16) {
    return std::make_shared<ARTNode16>(children);
  } else if (children.size() <= 48) {
    return std::make_shared<ARTNode48>(children);
  } else {
    return std::make_shared<ARTNode256>(children);
  }
}

std::vector<std::shared_ptr<const BaseSegment>> AdaptiveRadixTreeIndex::_get_indexed_segments() const {
  return {_indexed_segment};
}

size_t AdaptiveRadixTreeIndex::_memory_consumption() const {
  // ToDo(anyone): If you use this index in combination with the Tuning subsystem, you need to properly implement this.
  Fail("AdaptiveRadixTreeIndex::_memory_consumption() is not implemented yet");
}

AdaptiveRadixTreeIndex::BinaryComparable::BinaryComparable(ValueID value) : _parts(sizeof(value)) {
  for (size_t byte_id = 1; byte_id <= _parts.size(); ++byte_id) {
    // grab the 8 least significant bits and put them at the front of the vector
    _parts[_parts.size() - byte_id] = static_cast<uint8_t>(value) & 0xFFu;
    // rightshift 8 bits
    value >>= 8;
  }
}

size_t AdaptiveRadixTreeIndex::BinaryComparable::size() const { return _parts.size(); }

uint8_t AdaptiveRadixTreeIndex::BinaryComparable::operator[](size_t position) const {
  Assert(position < _parts.size(), "BinaryComparable indexed out of bounds");

  return _parts[position];
}

bool operator==(const AdaptiveRadixTreeIndex::BinaryComparable& left,
                const AdaptiveRadixTreeIndex::BinaryComparable& right) {
  if (left.size() != right.size()) return false;
  for (size_t i = 0; i < left.size(); ++i) {
    if (left[i] != right[i]) return false;
  }
  return true;
}

}  // namespace opossum
