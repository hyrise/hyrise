#pragma once

#include <boost/iterator/iterator_facade.hpp>
#include "utils/assert.hpp"

namespace opossum {

static constexpr size_t SERVER_BUFFER_SIZE = 4096u;

// This class implements an iterator on an array to let it behave like a circular buffer. If all data has been
// processed and the end of the underlying data structure is reached, the iterator wraps around and starts at the
// beginning of the data structure.
class RingBufferIterator : public boost::iterator_facade<RingBufferIterator, char, std::forward_iterator_tag, char&> {
 public:
  explicit RingBufferIterator(std::array<char, SERVER_BUFFER_SIZE>& data, size_t position = 0)
      : _data(data), _position(position) {}

  RingBufferIterator(const RingBufferIterator&) = default;

  RingBufferIterator& operator=(const RingBufferIterator& other) {
    DebugAssert(&_data == &other._data, "Cannot convert iterators from different arrays");
    _position = other._position;
    return *this;
  }

 private:
  friend class boost::iterator_core_access;

  // We have a couple of NOLINTs here because the facade expects these method names:

  bool equal(RingBufferIterator const& other) const {  // NOLINT
    return &_data == &other._data && _position == other._position;
  }

  void increment() {  // NOLINT
    _position = (_position + 1) % SERVER_BUFFER_SIZE;
  }

  reference dereference() const {  // NOLINT
    return _data[_position];
  }

  std::array<char, SERVER_BUFFER_SIZE>& _data;
  size_t _position;
};

}  // namespace opossum
