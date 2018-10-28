#pragma once

#include "all_type_variant.hpp"
#include "b_tree_index_impl.hpp"
#include "storage/base_segment.hpp"
#include "storage/index/base_index.hpp"
#include "types.hpp"

namespace opossum {

class BTreeIndexTest;

class BTreeIndex : public BaseIndex {
  friend BTreeIndexTest;

 public:
  using Iterator = std::vector<ChunkOffset>::const_iterator;

  /**
   * Predicts the memory consumption in bytes of creating this index.
   * See BaseIndex::estimate_memory_consumption()
   */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  BTreeIndex() = delete;
  explicit BTreeIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index);

 protected:
  Iterator _lower_bound(const std::vector<AllTypeVariant>&) const override;
  Iterator _upper_bound(const std::vector<AllTypeVariant>&) const override;
  Iterator _cbegin() const override;
  Iterator _cend() const override;
  std::vector<std::shared_ptr<const BaseSegment>> _get_indexed_segments() const override;
  size_t _memory_consumption() const override;

  std::shared_ptr<const BaseSegment> _indexed_segments;
  std::shared_ptr<BaseBTreeIndexImpl> _impl;
};

}  // namespace opossum
