#pragma once

#ifdef __clang__
#pragma clang diagnostic ignored "-Wall"
#include <btree_map.h>
#pragma clang diagnostic pop
#elif __GNUC__
#pragma GCC system_header
#include <btree_map.h>
#endif

#include "all_type_variant.hpp"
#include "storage/base_segment.hpp"
#include "types.hpp"

namespace opossum {

class BTreeIndexTest;

class BaseBTreeIndexImpl {
  friend BTreeIndexTest;

 public:
  BaseBTreeIndexImpl() = default;
  BaseBTreeIndexImpl(BaseBTreeIndexImpl&&) = default;
  BaseBTreeIndexImpl& operator=(BaseBTreeIndexImpl&&) = default;
  virtual ~BaseBTreeIndexImpl() = default;

  using Iterator = std::vector<ChunkOffset>::const_iterator;
  virtual size_t memory_consumption() const = 0;
  virtual Iterator lower_bound(const std::vector<AllTypeVariant>&) const = 0;
  virtual Iterator upper_bound(const std::vector<AllTypeVariant>&) const = 0;
  virtual Iterator cbegin() const = 0;
  virtual Iterator cend() const = 0;

 protected:
  std::vector<ChunkOffset> _chunk_offsets;
};

/**
* Implementation: https://code.google.com/archive/p/cpp-btree/
* Note: does not support null values right now.
*/
template <typename DataType>
class BTreeIndexImpl : public BaseBTreeIndexImpl {
  friend BTreeIndexTest;

 public:
  BTreeIndexImpl() = delete;
  ~BTreeIndexImpl() = default;
  explicit BTreeIndexImpl(const std::shared_ptr<const BaseSegment>& segments_to_index);

  BTreeIndexImpl(const BTreeIndexImpl&) = delete;
  BTreeIndexImpl& operator=(const BTreeIndexImpl&) = delete;

  BTreeIndexImpl(BTreeIndexImpl&&) = default;
  BTreeIndexImpl& operator=(BTreeIndexImpl&&) = default;

  size_t memory_consumption() const override;

  Iterator lower_bound(DataType value) const;
  Iterator upper_bound(DataType value) const;

  Iterator lower_bound(const std::vector<AllTypeVariant>&) const override;
  Iterator upper_bound(const std::vector<AllTypeVariant>&) const override;
  Iterator cbegin() const override;
  Iterator cend() const override;

 protected:
  void _bulk_insert(const std::shared_ptr<const BaseSegment>&);
  void _add_to_heap_memory_usage(const DataType&);

  btree::btree_map<DataType, size_t> _btree;
  size_t _heap_bytes_used;
};

}  // namespace opossum
