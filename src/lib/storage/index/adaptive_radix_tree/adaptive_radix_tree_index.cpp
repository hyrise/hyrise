#include "adaptive_radix_tree_index.hpp"

#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "adaptive_radix_tree_nodes.hpp"

#include "storage/base_attribute_vector.hpp"
#include "storage/base_column.hpp"
#include "storage/base_dictionary_column.hpp"
#include "storage/index/base_index.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

AdaptiveRadixTreeIndex::AdaptiveRadixTreeIndex(const std::vector<std::shared_ptr<BaseColumn>> &index_columns)
    : _index_column(std::dynamic_pointer_cast<BaseDictionaryColumn>(index_columns.front())) {
  DebugAssert(static_cast<bool>(_index_column), "AdaptiveRadixTree only works with DictionaryColumns for now");
  DebugAssert((index_columns.size() == 1), "AdaptiveRadixTree only works with a single column");

  // for each valueID in the attribute vector, create a pair consisting of a BinaryComparable of this valueID and its
  // ChunkOffset (needed for bulk-inserting)
  std::vector<std::pair<BinaryComparable, ChunkOffset>> pairs_to_insert;
  pairs_to_insert.reserve(_index_column->attribute_vector()->size());
  for (ChunkOffset chunk_offset = 0u; chunk_offset < _index_column->attribute_vector()->size(); ++chunk_offset) {
    pairs_to_insert.emplace_back(
        std::make_pair(BinaryComparable(_index_column->attribute_vector()->get(chunk_offset)), chunk_offset));
  }
  _root = _bulk_insert(pairs_to_insert);
}

BaseIndex::Iterator AdaptiveRadixTreeIndex::_lower_bound(const std::vector<AllTypeVariant> &values) const {
  assert(values.size() == 1);
  ValueID valueID = _index_column->lower_bound(values[0]);
  if (valueID == INVALID_VALUE_ID) {
    return _chunk_offsets.end();
  }
  return _root->lower_bound(BinaryComparable(valueID), 0);
}

BaseIndex::Iterator AdaptiveRadixTreeIndex::_upper_bound(const std::vector<AllTypeVariant> &values) const {
  assert(values.size() == 1);
  ValueID valueID = _index_column->upper_bound(values[0]);
  if (valueID == INVALID_VALUE_ID) {
    return _chunk_offsets.end();
  } else {
    return _root->lower_bound(BinaryComparable(valueID), 0);
  }
}

BaseIndex::Iterator AdaptiveRadixTreeIndex::_cbegin() const { return _chunk_offsets.cbegin(); }

BaseIndex::Iterator AdaptiveRadixTreeIndex::_cend() const { return _chunk_offsets.cend(); }

std::shared_ptr<Node> AdaptiveRadixTreeIndex::_bulk_insert(
    const std::vector<std::pair<BinaryComparable, ChunkOffset>> &values) {
  DebugAssert(!(values.empty()), "Index on empty column is not defined");
  _chunk_offsets.reserve(values.size());
  Iterator begin = _chunk_offsets.cbegin();
  return _bulk_insert(values, static_cast<size_t>(0u), begin);
}

std::shared_ptr<Node> AdaptiveRadixTreeIndex::_bulk_insert(
    const std::vector<std::pair<BinaryComparable, ChunkOffset>> &values, size_t depth, BaseIndex::Iterator &it) {
  // This is the anchor of the recursion: if all values have the same key, create a leaf.
  if (std::all_of(values.begin(), values.end(), [&values](const std::pair<BinaryComparable, ChunkOffset> &pair) {
        return values.front().first == pair.first;
      })) {
    // copy the Iterator in the _chunk_offsets - vector --> this is the lower_bound of the leaf
    Iterator lower = it;
    // insert the ChunkOffsets into the vector and push the Iterator further
    auto cap = _chunk_offsets.capacity();
    for (const auto &pair : values) {
      _chunk_offsets.emplace_back(pair.second);
    }
    std::advance(it, values.size());
    auto cap2 = _chunk_offsets.capacity();
    // we are not allowed to change the size of the vector as it would invalidate all our Iterators
    Assert(cap == cap2, "_chunk_offsets capacity changes, all Iterators are invalidated");

    // "it" points to the position after the last inserted ChunkOffset --> this is the upper_bound of the leave
    Iterator upper = it;
    return std::make_shared<Leaf>(lower, upper);
  }

  // radix-partition on the depths-byte into 256 partitions
  std::array<std::vector<std::pair<BinaryComparable, ChunkOffset>>, std::numeric_limits<uint8_t>::max() + 1> partitions;
  for (const auto &pair : values) {
    partitions[pair.first[depth]].emplace_back(pair);
  }

  // call recursively for each non-empty partition and gather the children
  std::vector<std::pair<uint8_t, std::shared_ptr<Node>>> children;

  for (uint16_t partition_id = 0; partition_id < partitions.size(); ++partition_id) {
    if (!partitions[partition_id].empty()) {
      auto child =
          std::make_pair(static_cast<uint8_t>(partition_id), _bulk_insert(partitions[partition_id], depth + 1, it));
      children.emplace_back(std::move(child));
    }
  }
  // finally create the appropriate Node according to the size of the children
  if (children.size() <= 4) {
    return std::make_shared<Node4>(children);
  } else if (children.size() <= 16) {
    return std::make_shared<Node16>(children);
  } else if (children.size() <= 48) {
    return std::make_shared<Node48>(children);
  } else {
    return std::make_shared<Node256>(children);
  }
}

std::vector<std::shared_ptr<BaseColumn>> AdaptiveRadixTreeIndex::_get_index_columns() const {
  std::vector<std::shared_ptr<BaseColumn>> v = {_index_column};
  return v;
}

AdaptiveRadixTreeIndex::BinaryComparable::BinaryComparable(ValueID value) : _parts(sizeof(value)) {
  for (size_t byte_id = 1; byte_id <= _parts.size(); ++byte_id) {
    // grab the 8 least significant bits and put them at the front of the vector
    _parts[_parts.size() - byte_id] = static_cast<uint8_t>(value & 0xFF);
    // rightshift 8 bits
    value >>= 8;
  }
}

size_t AdaptiveRadixTreeIndex::BinaryComparable::size() const { return _parts.size(); }

uint8_t AdaptiveRadixTreeIndex::BinaryComparable::operator[](size_t position) const {
  Assert(position < _parts.size(), "BinaryComparable indexed out of bounds");

  return _parts[position];
}

bool operator==(const AdaptiveRadixTreeIndex::BinaryComparable &lhs,
                const AdaptiveRadixTreeIndex::BinaryComparable &rhs) {
  if (lhs.size() != rhs.size()) return false;
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) return false;
  }
  return true;
}

}  // namespace opossum
