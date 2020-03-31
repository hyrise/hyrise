#include "rowid_pos_list.hpp"

namespace opossum {

void RowIDPosList::guarantee_single_chunk() { _references_single_chunk = true; }

bool RowIDPosList::references_single_chunk() const {
  if (_references_single_chunk) {
    DebugAssert(
        [&]() {
          if (size() == 0) return true;
          const auto& common_chunk_id = (*this)[0].chunk_id;
          return std::all_of(cbegin(), cend(), [&](const auto& row_id) {
            return row_id.chunk_id == common_chunk_id && row_id.chunk_offset != INVALID_CHUNK_OFFSET;
          });
        }(),
        "RowIDPosList was marked as referencing a single chunk, but references more");
  }
  return _references_single_chunk;
}

ChunkID RowIDPosList::common_chunk_id() const {
  DebugAssert(references_single_chunk(),
              "Can only retrieve the common_chunk_id if the RowIDPosList is guaranteed to reference a single chunk.");
  Assert(!empty(), "Cannot retrieve common_chunk_id of an empty chunk");
  Assert((*this)[0].chunk_id != INVALID_CHUNK_ID, "RowIDPosList that references_single_chunk must not contain NULL");
  return (*this)[0].chunk_id;
}

size_t RowIDPosList::memory_usage(const MemoryUsageCalculationMode mode) const {
  // Ignoring MemoryUsageCalculationMode because accurate calculation is efficient.
  return size() * sizeof(Vector::value_type);
}

}  // namespace opossum
