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

  BTreeIndex() = delete;
  explicit BTreeIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index);

  virtual uint64_t memory_consumption() const;

 protected:
  Iterator _lower_bound(const std::vector<AllTypeVariant>&) const override;
  Iterator _upper_bound(const std::vector<AllTypeVariant>&) const override;
  Iterator _cbegin() const override;
  Iterator _cend() const override;
  std::vector<std::shared_ptr<const BaseSegment>> _get_indexed_segments() const override;

  std::shared_ptr<const BaseSegment> _indexed_segments;
  std::shared_ptr<BaseBTreeIndexImpl> _impl;
};

}  // namespace opossum
