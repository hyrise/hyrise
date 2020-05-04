#include "abstract_pos_list.hpp"

namespace opossum {
AbstractPosList::PosListIterator<> AbstractPosList::begin() const {
  PerformanceWarning("Unresolved iterator created for AbstractPosList");
  return PosListIterator<>(this, ChunkOffset{0});
}

AbstractPosList::PosListIterator<> AbstractPosList::end() const {
  return PosListIterator<>(this, static_cast<ChunkOffset>(size()));
}

AbstractPosList::PosListIterator<> AbstractPosList::cbegin() const { return begin(); }

AbstractPosList::PosListIterator<> AbstractPosList::cend() const { return end(); }
}  // namespace opossum
