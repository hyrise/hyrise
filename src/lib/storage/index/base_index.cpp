#include "base_index.hpp"

#include <memory>
#include <vector>

namespace opossum {
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

ColumnIndexType BaseIndex::type() const { return _type(); }

}  // namespace opossum
