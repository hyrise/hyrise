#pragma once

#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "storage/index/base_index.hpp"
#include "types.hpp"

namespace opossum {

class BaseSegment;
class ARTNode;
class BaseDictionarySegment;

/**
 * The AdaptiveRadixTreeIndex (ART) currently works on single DictionarySegments. Conceptually it also works on
 * ValueSegments.
 * The ART does not compare full keys, but only partial keys in each node: On level n, the n-th byte of the full key
 * is compared.
 * In order to store the partial keys, it uses 4 different node-types, which can hold up to 4, 16, 48 and 256 partial
 * keys respectively.
 * Each node has an array which contains pointers to its children and (if needed) an index array in order to map
 * partial keys to positions in the array of the child-pointers
 *
 * The full specification of an ART can be found in the following paper: https://db.in.tum.de/~leis/papers/ART.pdf
 *
 * Find more information about this in our wiki: https://github.com/hyrise/hyrise/wiki/AdaptiveRadixTree-(ART)-Index
 *
 */
class AdaptiveRadixTreeIndex : public BaseIndex {
  friend class AdaptiveRadixTreeIndexTest;

  friend class AdaptiveRadixTreeIndexTest_BulkInsert_Test;

  friend class AdaptiveRadixTreeIndexTest_BinaryComparableFromChunkOffset_Test;

 public:
  explicit AdaptiveRadixTreeIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index);

  AdaptiveRadixTreeIndex(AdaptiveRadixTreeIndex&&) = default;

  AdaptiveRadixTreeIndex& operator=(AdaptiveRadixTreeIndex&&) = default;

  virtual ~AdaptiveRadixTreeIndex() = default;

  /**
   *All keys in the ART have to be binary comparable in the sense that if the most significant differing bit between
   *BinaryComparable a and BinaryComparable b is greater for a <=> a > b.
   *This is true for unsigned values (like the ValueID), but signed values, chars and strings have to be transformed
   *in order to fulfill this property. The BinaryComparable class works as a common interface for those values.
   *The ART compares keys byte-wise, therefore we save the bytes of a BinaryComparable in a vector.
   */

  class BinaryComparable {
   public:
    explicit BinaryComparable(ValueID value);

    size_t size() const;

    uint8_t operator[](size_t position) const;

   private:
    std::vector<uint8_t> _parts;
  };

 private:
  Iterator _lower_bound(const std::vector<AllTypeVariant>& values) const final;

  Iterator _upper_bound(const std::vector<AllTypeVariant>& values) const final;

  Iterator _cbegin() const final;

  Iterator _cend() const final;

  std::shared_ptr<ARTNode> _bulk_insert(const std::vector<std::pair<BinaryComparable, ChunkOffset>>& values);

  std::shared_ptr<ARTNode> _bulk_insert(const std::vector<std::pair<BinaryComparable, ChunkOffset>>& values,
                                        size_t depth, Iterator& it);

  std::vector<std::shared_ptr<const BaseSegment>> _get_indexed_segments() const;

  const std::shared_ptr<const BaseDictionarySegment> _indexed_segment;
  std::vector<ChunkOffset> _chunk_offsets;
  std::shared_ptr<ARTNode> _root;
};

bool operator==(const AdaptiveRadixTreeIndex::BinaryComparable& left,
                const AdaptiveRadixTreeIndex::BinaryComparable& right);
}  // namespace opossum
