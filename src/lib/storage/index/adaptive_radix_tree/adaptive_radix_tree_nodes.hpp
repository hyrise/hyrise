#pragma once

#include <array>
#include <functional>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "adaptive_radix_tree_index.hpp"

#include "types.hpp"

namespace opossum {

using Iterator = std::vector<ChunkOffset>::const_iterator;

/**
 * This file declares the ARTNode-types needed for the Adaptive-Radix-Tree (ART)
 * In order to store its partial keys, the ART uses 4 different node-types, which can hold up to 4, 16, 48 and 256
 * partial keys respectively.
 * Each node has an array which contains pointers to its children and (if needed) an index array in order to map
 * partial keys to positions in the array of the child-pointers
 *
 */

class ARTNode : private Noncopyable {
 public:
  ARTNode() = default;

  virtual ~ARTNode() = default;

  virtual Iterator lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const = 0;
  virtual Iterator upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const = 0;
  virtual Iterator begin() const = 0;
  virtual Iterator end() const = 0;
};

/**
 *
 * ARTNode4 has two arrays of length 4:
 *  - _partial_keys stores the contained partial_keys of its children
 *  - _children stores pointers to the children
 *
 * _partial_key[i] is the partial_key for child _children[i]
 *
 * The default value of the _partial_keys array is 255u
 */
class ARTNode4 final : public ARTNode {
  friend class AdaptiveRadixTreeIndexTest_BulkInsert_Test;

 public:
  explicit ARTNode4(std::vector<std::pair<uint8_t, std::shared_ptr<ARTNode>>>& children);

  Iterator lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const override;
  Iterator upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const override;
  Iterator begin() const override;
  Iterator end() const override;

 private:
  /**
   * _delegate_to_child is used to avoid code duplication between lower_bound() and upper_bound as the access to the
   * children
   * is the same, only the method called on the matching child differs
   */
  Iterator _delegate_to_child(
      const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth,
      const std::function<Iterator(size_t, const AdaptiveRadixTreeIndex::BinaryComparable&, size_t)>& function) const;
  std::array<uint8_t, 4> _partial_keys;
  std::array<std::shared_ptr<ARTNode>, 4> _children;
};

/**
 *
 * ARTNode16 has two arrays of length 16, very similar to ARTNode4:
 *  - _partial_keys stores the contained partial_keys of its children
 *  - _children stores pointers to the children
 *
 * _partial_key[i] is the partial_key for child _children[i]
 *
 * The default value of the _partial_keys array is 255u
 *
 */

class ARTNode16 final : public ARTNode {
 public:
  explicit ARTNode16(std::vector<std::pair<uint8_t, std::shared_ptr<ARTNode>>>& children);

  Iterator lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const override;
  Iterator upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const override;
  Iterator begin() const override;
  Iterator end() const override;

 private:
  Iterator _delegate_to_child(
      const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth,
      const std::function<Iterator(std::iterator_traits<std::array<uint8_t, 16>::iterator>::difference_type,
                                   const AdaptiveRadixTreeIndex::BinaryComparable&, size_t)>& function) const;
  std::array<uint8_t, 16> _partial_keys;
  std::array<std::shared_ptr<ARTNode>, 16> _children;
};

/**
 *
 * ARTNode48 has two arrays:
 *  - _index_to_child of length 256 that can be directly addressed
 *  - _children of length 48 stores pointers to the children
 *
 * _index_to_child[partial_key] stores the index for the child in _children
 *
 * The default value of the _index_to_child array is 255u. This is safe as the maximum value set in _index_to_child
 * will
 * be
 * 47 as this is the maximum index for _children.
 */
class ARTNode48 final : public ARTNode {
 public:
  explicit ARTNode48(const std::vector<std::pair<uint8_t, std::shared_ptr<ARTNode>>>& children);

  Iterator lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const override;
  Iterator upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const override;
  Iterator begin() const override;
  Iterator end() const override;

 private:
  Iterator _delegate_to_child(
      const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth,
      const std::function<Iterator(uint8_t, const AdaptiveRadixTreeIndex::BinaryComparable&, size_t)>& function) const;

  std::array<uint8_t, 256> _index_to_child;
  std::array<std::shared_ptr<ARTNode>, 48> _children;
};

/**
 *
 * ARTNode256 has only one array: _children; which stores pointers to the children and can be directly addressed.
 *
 */
class ARTNode256 final : public ARTNode {
 public:
  explicit ARTNode256(const std::vector<std::pair<uint8_t, std::shared_ptr<ARTNode>>>& children);

  Iterator lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const override;
  Iterator upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const override;
  Iterator begin() const override;
  Iterator end() const override;

 private:
  Iterator _delegate_to_child(
      const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth,
      const std::function<Iterator(uint8_t, AdaptiveRadixTreeIndex::BinaryComparable, size_t)>& function) const;

  std::array<std::shared_ptr<ARTNode>, 256> _children;
};

/**
 *
 * Leafs contain two std::vector<ChunkOffset>::const_iterator: _lower_bound and _upper_bound.
 *
 * Consider the following example tree showing only its leafs:
 *
 *           Leaf(0x00000000) Leaf(0x00000001) ... Leaf(0xa101fe07) Leaf(0xa101feaf) ... Leaf(0xfebb34f1)
 *         _lower_ |  | _upper_     |       | _upper_    | |             |       |           |          |
 *           bound |  | bound   |---|       | bound      | |         |---|       |           |          |
 *                 |  |--------|| _lower_   |--|      |--| |--------||           |      |----|          |
 *                 |           || bound        |      |             ||           |      |               |
 * _chunk_offsets: |17|a2|a4|b4|fe|02|03|04|a1|a3|...|12|c1|f3|1a|4f|6d|...|92|9a|27|...|00|13|aa|ab|f1|
 *
 *
 * _lower_bound points to the first ChunkOffset in _chunk_offsets that belongs to the value in the attribute vector
 * that
 * the leaf represents.
 *     eg: on ChunkOffset 17 the value is 0x00000000, on ChunkOffset f3 the value is 0xa101fe07
 *
 * _upper_bound points to the first ChunkOffset in _chunk_offsets that does not contain the value that the leaf
 * represents.
 *     eg: at ChunkOffset fe, the value is 0x00000001, not 0x00000000
 *     for the last leaf, _upper_bound = _chunk_offsets.end()
 *
 * lower_bound() nets the same as begin(), upper_bound the same as end()
 */
class Leaf final : public ARTNode {
  friend class AdaptiveRadixTreeIndexTest_BulkInsert_Test;

 public:
  explicit Leaf(Iterator& lower, Iterator& upper);

  Iterator lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable&, size_t) const override;
  Iterator upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable&, size_t) const override;
  Iterator begin() const override;
  Iterator end() const override;

 private:
  Iterator _begin;
  Iterator _end;
};

}  // namespace opossum
