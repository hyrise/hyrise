#include "abstract_ordered_index.hpp"

namespace opossum {

AbstractOrderedIndex::AbstractOrderedIndex(const IndexType type) : AbstractIndex<ChunkOffset>(type) {}

AbstractOrderedIndex::Iterator AbstractOrderedIndex::lower_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert(
      (_get_indexed_segments().size() >= values.size()),
      "AbstractOrderedIndex: The number of queried segments has to be less or equal to the number of indexed segments.");

  return _lower_bound(values);
}

AbstractOrderedIndex::Iterator AbstractOrderedIndex::upper_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert(
      (_get_indexed_segments().size() >= values.size()),
      "AbstractOrderedIndex: The number of queried segments has to be less or equal to the number of indexed segments.");

  return _upper_bound(values);
}

}  // namespace opossum
