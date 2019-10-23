#pragma once

#include <array>
#include <boost/asio.hpp>  // NOLINT
#include "postgres_message_types.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

using Socket = boost::asio::ip::tcp::socket;

static constexpr size_t BUFFER_SIZE = 4096u;

// This class implements a circular buffer. If all data has been processed and the end of the underlying data
// structure is reached, the iterator wraps around and starts at the beginning of the data structure.
class RingBufferIterator
    : public boost::iterator_facade<RingBufferIterator, char, std::random_access_iterator_tag, char&> {
 public:
  explicit RingBufferIterator(std::array<char, BUFFER_SIZE>& data, size_t position = 0)
      : _data(data), _position(position) {}

  RingBufferIterator(const RingBufferIterator&) = default;

  RingBufferIterator& operator=(const RingBufferIterator& other) {
    DebugAssert(&_data == &other._data, "Can't convert iterators from different arrays");
    _position = other._position;
    return *this;
  }

 private:
  friend class boost::iterator_core_access;

  // We have a couple of NOLINTs here because the facade expects these method names:

  bool equal(RingBufferIterator const& other) const {  // NOLINT
    return &_data == &other._data && _position == other._position;
  }

  std::ptrdiff_t distance_to(RingBufferIterator const& other) const {  // NOLINT
    DebugAssert(&_data == &other._data, "Can't convert iterators from different arrays");
    return std::ptrdiff_t(other._position - _position);
  }

  void advance(size_t increment) {  // NOLINT
    _position = (_position + increment) % BUFFER_SIZE;
  }

  void increment() {  // NOLINT
    _position = (_position + 1) % BUFFER_SIZE;
  }

  void decrement() {  // NOLINT
    _position = _position == 0 ? BUFFER_SIZE - 1 : _position - 1;
  }

  char& dereference() const {  // NOLINT
    return _data[_position];
  }

  std::array<char, BUFFER_SIZE>& _data;
  size_t _position;
};

// Dedicated buffer for read operations. The ring buffer gets extended by methods for reading different data types.
template <typename SocketType>
class ReadBuffer {
 public:
  explicit ReadBuffer(std::shared_ptr<SocketType> socket) : _socket(socket) {}

  // Problem: full and empty might be same state, so head == tail
  // Solution: Full state is tail + 1 == head
  //           Empty state is head == tail
  size_t size() const {
    const auto current_size = _current_position - _start_position;
    if (current_size < 0) {
      return current_size + BUFFER_SIZE;
    } else {
      return current_size;
    }
  }

  // See comment above
  size_t maximum_capacity() const { return BUFFER_SIZE - 1; }

  bool full() const { return size() == maximum_capacity(); }

  // Extract numerical values from buffer. Values will be converted into the correct byte order if necessary.
  template <typename T>
  T get_value() {
    _receive_if_necessary(sizeof(T));
    T network_value = 0;
    std::copy_n(_start_position, sizeof(T), reinterpret_cast<char*>(&network_value));
    _start_position += sizeof(T);
    if constexpr (std::is_same_v<T, uint16_t> || std::is_same_v<T, int16_t>) {
      return ntohs(network_value);
    } else if constexpr (std::is_same_v<T, uint32_t> || std::is_same_v<T, int32_t>) {
      return ntohl(network_value);
    } else {
      return network_value;
    }
  }

  // String functions
  std::string get_string(const size_t string_length,
                         const IgnoreNullTerminator ignore_null_terminator = IgnoreNullTerminator::No);
  std::string get_string();

  PostgresMessageType get_message_type();

 private:
  std::array<char, BUFFER_SIZE> _data;
  RingBufferIterator _start_position{_data};
  RingBufferIterator _current_position{_data};
  std::shared_ptr<SocketType> _socket;
  void _receive_if_necessary(const size_t bytes_required = 1);
};

// Dedicated buffer for write operations. The ring buffer gets extended by methods for writing different data types.
template <typename SocketType>
class WriteBuffer {
 public:
  explicit WriteBuffer(const std::shared_ptr<SocketType> socket) : _socket(socket) {}

  // Problem: full and empty might be same state, so head == tail
  // Solution: Full state is tail + 1 == head
  //           Empty state is head == tail
  size_t size() const {
    const auto current_size = _current_position - _start_position;
    if (current_size < 0) {
      return current_size + BUFFER_SIZE;
    } else {
      return current_size;
    }
  }

  // See comment above
  size_t maximum_capacity() const { return BUFFER_SIZE - 1; }

  bool full() const { return size() == maximum_capacity(); }

  // Flush whole buffer, e. g. after a finished request
  void flush(const size_t bytes_required = 0);

  // Put string into the buffer. If the string is longer than the buffer itself the buffer will flush automatically.
  void put_string(const std::string& value,
                  const IgnoreNullTerminator ignore_null_terminator = IgnoreNullTerminator::No);

  // Put numerical values into the buffer. Values will be converted into the correct byte order if necessary.
  template <typename T>
  void put_value(const T network_value) {
    _flush_if_necessary(sizeof(T));
    T converted_value;
    if constexpr (std::is_same_v<T, uint16_t> || std::is_same_v<T, int16_t>) {
      converted_value = htons(network_value);
    } else if constexpr (std::is_same_v<T, uint32_t> || std::is_same_v<T, int32_t>) {
      converted_value = htonl(network_value);
    } else {
      converted_value = network_value;
    }
    std::copy_n(reinterpret_cast<const char*>(&converted_value), sizeof(T), _current_position);
    _current_position += sizeof(T);
  }

 private:
  std::array<char, BUFFER_SIZE> _data;
  RingBufferIterator _start_position{_data};
  RingBufferIterator _current_position{_data};
  std::shared_ptr<SocketType> _socket;
  void _flush_if_necessary(const size_t bytes_required);
};

}  // namespace opossum
