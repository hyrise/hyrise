#pragma once

#include <array>

#include <boost/iterator/iterator_facade.hpp>

#include "utils/assert.hpp"

namespace hyrise {

static constexpr auto SERVER_BUFFER_SIZE = size_t{4096};

// This class implements an iterator on an array to let it behave like a circular buffer. If all data has been
// processed and the end of the underlying data structure is reached, the iterator wraps around and starts at the
// beginning of the data structure.
class RingBufferIterator : public boost::iterator_facade<RingBufferIterator, char, std::forward_iterator_tag, char&> {
 public:
  RingBufferIterator(RingBufferIterator&&) = default;
  ~RingBufferIterator() = default;

  explicit RingBufferIterator(std::array<char, SERVER_BUFFER_SIZE>& data, size_t position = 0)
      : _data(data), _position(position) {}

  RingBufferIterator(const RingBufferIterator&) = default;

  RingBufferIterator& operator=(const RingBufferIterator& other) {
    if (this == &other) {
      return *this;
    }
    DebugAssert(&_data == &other._data, "Cannot convert iterators from different arrays.");
    _position = other._position;
    return *this;
  }

  // NOLINTNEXTLINE(cppcoreguidelines-noexcept-move-operations,hicpp-noexcept-move,performance-noexcept-move-constructor)
  RingBufferIterator& operator=(RingBufferIterator&& other) {
    if (this == &other) {
      return *this;
    }
    DebugAssert(&_data == &other._data, "Cannot convert iterators from different arrays.");
    _position = other._position;
    return *this;
  }

 private:
  friend class boost::iterator_core_access;

  // We have a couple of NOLINTs here because the facade expects these method names:

  bool equal(RingBufferIterator const& other) const {  // NOLINT(readability-identifier-naming)
    return &_data == &other._data && _position == other._position;
  }

  void increment() {  // NOLINT(readability-identifier-naming)
    _position = (_position + 1) % SERVER_BUFFER_SIZE;
  }

  reference dereference() const {  // NOLINT(readability-identifier-naming)
    return _data[_position];
  }

  std::array<char, SERVER_BUFFER_SIZE>& _data;
  size_t _position;
};

}  // namespace hyrise
