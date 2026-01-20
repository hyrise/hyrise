#pragma once

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace hyrise {

class RowIDPosList;

class AbstractPosList : private Noncopyable {
 public:
  template <typename PosListType = AbstractPosList, typename DereferenceReturnType = RowID>
  class PosListIterator : public boost::iterator_facade<PosListIterator<PosListType, DereferenceReturnType>, RowID,
                                                        boost::random_access_traversal_tag, DereferenceReturnType> {
   public:
    PosListIterator(const PosListType* init_pos_list, const ChunkOffset offset)
        : pos_list(init_pos_list), chunk_offset(offset) {}

    void increment() {
      ++chunk_offset;
    }

    void decrement() {
      --chunk_offset;
    }

    void advance(std::ptrdiff_t distance) {
      chunk_offset += distance;
    }

    bool equal(const PosListIterator& other) const {
      DebugAssert(pos_list == other.pos_list, "PosListIterator compared to iterator on different PosList instance.");
      return other.chunk_offset == chunk_offset;
    }

    std::ptrdiff_t distance_to(const PosListIterator& other) const {
      return static_cast<std::ptrdiff_t>(other.chunk_offset) - chunk_offset;
    }

    DereferenceReturnType dereference() const {
      DebugAssert(chunk_offset < pos_list->size(), "Past-the-end PosListIterator dereferenced.");
      return (*pos_list)[chunk_offset];
    }

    const PosListType* pos_list;
    ChunkOffset chunk_offset;
  };

  // Returns whether it is guaranteed that the PosList references a single ChunkID.
  // However, it may be false even if this is the case.
  virtual bool references_single_chunk() const = 0;

  // For chunks that share a common ChunkID, returns that ID.
  virtual ChunkID common_chunk_id() const = 0;

  virtual RowID operator[](size_t distance) const = 0;

  PosListIterator<> begin() const;
  PosListIterator<> end() const;
  PosListIterator<> cbegin() const;
  PosListIterator<> cend() const;

  // Capacity
  virtual bool empty() const = 0;
  virtual size_t size() const = 0;

  virtual size_t memory_usage(const MemoryUsageCalculationMode) const = 0;

  friend bool operator==(const AbstractPosList& lhs, const AbstractPosList& rhs);
};

inline bool operator==(const AbstractPosList& lhs, const AbstractPosList& rhs) {
  PerformanceWarning("Using slow PosList comparison.");
  // NOLINTNEXTLINE(modernize-use-ranges)
  return std::equal(lhs.cbegin(), lhs.cend(), rhs.cbegin(), rhs.cend());
}

}  // namespace hyrise
