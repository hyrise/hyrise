#pragma once

#ifdef __clang__
  #pragma clang diagnostic ignored "-Wall"
  #include <btree_map.h>
  #pragma clang diagnostic pop
#elif __GNUC__
  #pragma GCC system_header
  #include <btree_map.h>
#endif


#include "types.hpp"
#include "all_type_variant.hpp"
#include "storage/base_column.hpp"

namespace opossum {


class BaseBTreeIndexImpl {
 public:
  BaseBTreeIndexImpl() = default;
  BaseBTreeIndexImpl(BaseBTreeIndexImpl&&) = default;
  BaseBTreeIndexImpl& operator=(BaseBTreeIndexImpl&&) = default;
  virtual ~BaseBTreeIndexImpl() = default;

  using Iterator = std::vector<ChunkOffset>::const_iterator;
  virtual Iterator lower_bound(const std::vector<AllTypeVariant>&) const = 0;
  virtual Iterator upper_bound(const std::vector<AllTypeVariant>&) const = 0;
  virtual Iterator cbegin() const = 0;
  virtual Iterator cend() const = 0;
};

/**
* Implementation: https://code.google.com/archive/p/cpp-btree/
* Note: does not support null values right now.
*/
template <typename DataType>
class BTreeIndexImpl : public BaseBTreeIndexImpl {
 public:
  BTreeIndexImpl() = delete;
  explicit BTreeIndexImpl(std::shared_ptr<const BaseColumn> index_column);

  BTreeIndexImpl(const BTreeIndexImpl&) = delete;
  BTreeIndexImpl& operator=(const BTreeIndexImpl&) = delete;

  BTreeIndexImpl(BTreeIndexImpl&&) = default;
  BTreeIndexImpl& operator=(BTreeIndexImpl&&) = default;

  virtual uint64_t memory_consumption() const;

  Iterator lower_bound(DataType value) const;
  Iterator upper_bound(DataType value) const;

  Iterator lower_bound(const std::vector<AllTypeVariant>&) const override;
  Iterator upper_bound(const std::vector<AllTypeVariant>&) const override;
  Iterator cbegin() const override;
  Iterator cend() const override;

 protected:
  void _bulk_insert(const std::shared_ptr<const BaseColumn>);

  btree::btree_map<DataType, size_t> _btree;
  std::vector<ChunkOffset> _chunk_offsets;
};

}  // namespace opossum
