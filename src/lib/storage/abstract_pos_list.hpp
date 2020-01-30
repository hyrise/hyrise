#pragma once

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// class AbstractPosList {
// // public:
// //   virtual ~AbstractPosList() = default;
// // virtual RowID operator[](size_t n) const = 0; 
// }

// template<typename Functor>
// void resolve_pos_list_type(const AbstractPosList& untyped_pos_list, const Functor& func);

class PosList;

class AbstractPosList {
public:
  template <bool modifiable = false>
  class PosListIterator : public boost::iterator_facade<PosListIterator<modifiable>, RowID, boost::random_access_traversal_tag, std::conditional_t<modifiable, RowID&, RowID>> {
public:
  typedef typename std::conditional<modifiable, AbstractPosList*, const AbstractPosList*>::type Typ;
  typedef typename std::conditional<modifiable, RowID&, RowID>::type DereferenceReturnType;
  PosListIterator(Typ pl_pointer, ChunkOffset pos, ChunkOffset max_size) {
    _pl_pointer = pl_pointer;
    _chunk_offset = pos;
    _max_size = max_size;
  }

  void increment() { ++_chunk_offset; 
    if (_chunk_offset > _max_size) {
      std::cout << "Sau" << std::endl;
    }
  }

  void decrement() { --_chunk_offset;
    if (_chunk_offset > _max_size) {
      std::cout << "Sau" << std::endl;
    }
   }

  void advance(std::ptrdiff_t n) { _chunk_offset += n;
    if (_chunk_offset > _max_size) {
      std::cout << "Sau" << std::endl;
    }
   }

  /**
   * Although `other` could have a different type, it is practically impossible,
   * since AnySegmentIterator is only used within AnySegmentIterable.
   */


  bool equal(const PosListIterator other) const {
    // assert + chunk offset compare
    // DebugAssert()
    // return false;
    return (other._chunk_offset == _chunk_offset && other._pl_pointer == _pl_pointer);
    // const auto casted_other = static_cast<const AnySegmentIteratorWrapper<T, Iterator>*>(other);
    // return _iterator == casted_other->_iterator;
  }

  bool equal(const PosListIterator* other) const {
    // assert + chunk offset compare
    // DebugAssert()
    return (other->_chunk_offset == _chunk_offset && other->_pl_pointer == _pl_pointer);
    // const auto casted_other = static_cast<const AnySegmentIteratorWrapper<T, Iterator>*>(other);
    // return _iterator == casted_other->_iterator;
  }

  std::ptrdiff_t distance_to(const PosListIterator other) const {
    // const auto casted_other = static_cast<const AnySegmentIteratorWrapper<T, Iterator>*>(other);
    // return casted_other->_iterator - _iterator;
    return (other._chunk_offset - _chunk_offset);
  }

  std::ptrdiff_t distance_to(const PosListIterator* other) const {
    // const auto casted_other = static_cast<const AnySegmentIteratorWrapper<T, Iterator>*>(other);
    // return casted_other->_iterator - _iterator;
    return (other->_chunk_offset - _chunk_offset);
  }

  DereferenceReturnType dereference() const {
    if constexpr (modifiable) {
      return static_cast<PosList&>(*_pl_pointer)[_chunk_offset];
    } else {
      return (*_pl_pointer)[_chunk_offset];
    }
    // const auto value = *_iterator;
    // return {value.value(), value.is_null(), value.chunk_offset()};

  }


  Typ _pl_pointer;
  ChunkOffset _chunk_offset;
  ChunkOffset _max_size;
};

 // public:
  virtual ~AbstractPosList() = default;

  AbstractPosList& operator=(AbstractPosList&& other) = default;

  // Returns whether it is guaranteed that the PosList references a single ChunkID.
  // However, it may be false even if this is the case.
  virtual bool references_single_chunk() const = 0;

  // For chunks that share a common ChunkID, returns that ID.
  virtual ChunkID common_chunk_id() const = 0;

  virtual RowID operator[](size_t n) const = 0;

  PosListIterator<false> begin() const {
    return PosListIterator<false>(this, ChunkOffset{0}, ChunkOffset{static_cast<ChunkOffset>(size())});
  }

  // PosListIterator<true> begin() {
  //   // if is debug{
  //   if (auto t = std::dynamic_cast<PosList>(this)) {
  //     return PosListIterator<true>(this, ChunkOffset{0});
  //   }
  // }

  PosListIterator<true> begin();
  PosListIterator<true> end();

      // PosListIterator<true>
    // return PosListIterator(this, ChunkOffset{0});
  // }

  PosListIterator<false> end() const {
    return PosListIterator<false>(this, ChunkOffset{static_cast<ChunkOffset>(size())}, ChunkOffset{static_cast<ChunkOffset>(size())});
  }

  PosListIterator<false> cbegin() const {
    return PosListIterator<false>(this, ChunkOffset{0}, ChunkOffset{static_cast<ChunkOffset>(size())});
  }

  PosListIterator<false> cend() const {
    return PosListIterator<false>(this, ChunkOffset{static_cast<ChunkOffset>(size())}, ChunkOffset{static_cast<ChunkOffset>(size())});
  }

  // Capacity
  virtual bool empty() const = 0;
  virtual size_t size() const = 0;

  virtual size_t memory_usage(const MemoryUsageCalculationMode) const = 0;

  virtual bool operator==(const AbstractPosList& other) const = 0;

  // template <typename Functor>
  // void for_each(const Functor& functor) const;
};

template <typename PosListType>
extern typename PosListType::const_iterator make_pos_list_begin_iterator(PosListType& pos_list);

template <typename PosListType>
extern typename PosListType::const_iterator make_pos_list_end_iterator(PosListType& pos_list);

template <typename PosListType>
extern typename PosListType::iterator make_pos_list_begin_iterator_nc(PosListType& pos_list);

template <typename PosListType>
extern typename PosListType::iterator make_pos_list_end_iterator_nc(PosListType& pos_list);

}  // namespace opossum
