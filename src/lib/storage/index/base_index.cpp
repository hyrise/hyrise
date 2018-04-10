#include "base_index.hpp"

#include <limits>
#include <memory>
#include <vector>

#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"

namespace opossum {

size_t BaseIndex::estimate_memory_consumption(ColumnIndexType type, ChunkOffset row_count, ChunkOffset value_count,
                                              uint32_t value_bytes) {
  switch (type) {
    case ColumnIndexType::GroupKey:
      return GroupKeyIndex::estimate_memory_consumption(row_count, value_count, value_bytes);
    case ColumnIndexType::CompositeGroupKey:
      return CompositeGroupKeyIndex::estimate_memory_consumption(row_count, value_count, value_bytes);
    case ColumnIndexType::AdaptiveRadixTree:
      return AdaptiveRadixTreeIndex::estimate_memory_consumption(row_count, value_count, value_bytes);
    default:
      Fail("estimate_memory_consumption() is not implemented for the given index type");
  }
}

BaseIndex::BaseIndex(const ColumnIndexType type) : _type{type} {}

bool BaseIndex::is_index_for(const std::vector<std::shared_ptr<const BaseColumn>>& columns) const {
  auto index_columns = _get_index_columns();
  if (columns.size() > index_columns.size()) return false;
  if (columns.empty()) return false;

  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i] != index_columns[i]) return false;
  }
  return true;
}

BaseIndex::Iterator BaseIndex::lower_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert((_get_index_columns().size() >= values.size()),
              "BaseIndex: The amount of queried columns has to be less or equal to the number of indexed columns.");

  return _lower_bound(values);
}

BaseIndex::Iterator BaseIndex::upper_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert((_get_index_columns().size() >= values.size()),
              "BaseIndex: The amount of queried columns has to be less or equal to the number of indexed columns.");

  return _upper_bound(values);
}

BaseIndex::Iterator BaseIndex::cbegin() const { return _cbegin(); }

BaseIndex::Iterator BaseIndex::cend() const { return _cend(); }

size_t BaseIndex::memory_consumption() const { return _memory_consumption(); }

ColumnIndexType BaseIndex::type() const { return _type; }

}  // namespace opossum
