#pragma once

#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class PosList;

class AbstractPosList {
 public:
  virtual ~AbstractPosList();

  // Returns whether it is guaranteed that the PosList references a single ChunkID.
  // However, it may be false even if this is the case.
  virtual bool references_single_chunk() const = 0;

  // For chunks that share a common ChunkID, returns that ID.
  virtual ChunkID common_chunk_id() const = 0;

  virtual RowID operator[](size_t n) const = 0;

  // Capacity
  virtual bool empty() const = 0;
  virtual size_t size() const = 0;

  virtual size_t memory_usage(const MemoryUsageCalculationMode) const = 0;

  virtual bool operator==(const AbstractPosList& other) const = 0;

  template <typename Functor>
  void for_each(const Functor& functor) const {
    resolve_pos_list_type(*this, [&functor](auto& pos_list){
      auto it = make_pos_list_begin_iterator(pos_list);
      auto end = make_pos_list_end_iterator(pos_list);
      for(; it != end; ++it) {
        functor(*it);
      }
    });
  }
};

class PosList;

template <typename PosListType>
typename PosListType::const_iterator make_pos_list_begin_iterator(PosListType& pos_list);

template <typename PosListType>
typename PosListType::const_iterator make_pos_list_end_iterator(PosListType& pos_list);


}  // namespace opossum
