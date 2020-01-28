#pragma once

#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class AbstractPosListIterator {
 public:
  using iterator_category = std::input_iterator_tag;
  using value_type = RowID;
  using difference_type = std::ptrdiff_t;
  using reference = const RowID&;
  using pointer = const RowID*;

  virtual ~AbstractPosListIterator();

  virtual AbstractPosListIterator& operator++() = 0;

  virtual AbstractPosListIterator& operator--() = 0;

  virtual AbstractPosListIterator& operator+=(size_t n) = 0;

  /* virtual AbstractPosListIterator operator+(size_t n) = 0; */

  virtual const AbstractPosListIterator& operator++(int) = 0;

  virtual difference_type operator-(const AbstractPosListIterator& other) const = 0;

  virtual bool operator==(const AbstractPosListIterator& other) const = 0;

  virtual bool operator!=(const AbstractPosListIterator& other) const = 0;

  virtual value_type operator*() const = 0;

  virtual pointer operator->() const = 0;
};

class AbstractPosList {
 public:
  virtual ~AbstractPosList();

  // Returns whether it is guaranteed that the PosList references a single ChunkID.
  // However, it may be false even if this is the case.
  virtual bool references_single_chunk() const = 0;

  // For chunks that share a common ChunkID, returns that ID.
  virtual ChunkID common_chunk_id() const = 0;

  virtual AbstractPosListIterator cbegin() const = 0;
  virtual AbstractPosListIterator cend() const = 0;

  virtual AbstractPosListIterator begin() const = 0;
  virtual AbstractPosListIterator end() const = 0;

  virtual RowID operator[](size_t n) const = 0;

  // Capacity
  virtual bool empty() const = 0;
  virtual size_t size() const = 0;

  virtual size_t memory_usage(const MemoryUsageCalculationMode) const = 0;

  virtual bool operator==(const AbstractPosList& other) const = 0;
};

}  // namespace opossum
