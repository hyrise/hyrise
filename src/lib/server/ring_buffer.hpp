#pragma once

#include <array>
#include <boost/asio.hpp>  // NOLINT
#include "network_message_types.hpp"

namespace opossum {

using Socket = boost::asio::ip::tcp::socket;

static constexpr size_t BUFFER_SIZE = 4096u;

// This class implements the logic of a circular buffer. If the end of the underlying data structure is reached
// the iterator will start at the beginning if the data has already been processed.
class RingBufferIterator : public std::iterator<std::forward_iterator_tag, char> {
 public:
  explicit RingBufferIterator(std::array<char, BUFFER_SIZE>& data, size_t position = 0)
      : _data(data), _position(position) {}

  RingBufferIterator& operator=(const RingBufferIterator& other) {
    _position = other._position;
    return *this;
  }

  RingBufferIterator(const RingBufferIterator& other) = default;

  bool operator==(RingBufferIterator other) const { return (_position == other._position) && (_data == other._data); }

  bool operator!=(RingBufferIterator other) const { return !(*this == other); }

  RingBufferIterator& operator++() {
    _position = (_position + 1) % BUFFER_SIZE;
    return *this;
  }

  RingBufferIterator operator++(int) {
    RingBufferIterator retval = *this;
    ++(*this);
    return retval;
  }

  RingBufferIterator& operator+(const size_t increment) {
    _position = (_position + increment) % BUFFER_SIZE;
    return *this;
  }

  RingBufferIterator& operator+=(const size_t increment) { return operator+(increment); }

  RingBufferIterator::difference_type operator-(RingBufferIterator const& other) const {
    return _position - other._position;
  }

  char& operator*() { return _data[_position]; }

  char* get_raw_pointer() { return &_data[_position]; }

 private:
  std::array<char, BUFFER_SIZE>& _data;
  size_t _position;
};

// This class implements general methods for the ring buffer.
class RingBuffer {
 public:
  RingBuffer() = default;

  char* data() noexcept { return _data.begin(); }

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

 protected:
  std::array<char, BUFFER_SIZE> _data;
  RingBufferIterator _start_position = RingBufferIterator(_data);
  RingBufferIterator _current_position = RingBufferIterator(_data);
};

// Dedicated buffer for read operations. The ring buffer gets extended by methods for reading different data types.
template<typename SocketType>
class ReadBuffer : public RingBuffer {
 public:
  explicit ReadBuffer(std::shared_ptr<SocketType> socket) : _socket(socket) {}

  // Extract numerical values from buffer. Values will be converted into the correct byte order if necessary.
  template <typename T, typename std::enable_if_t<std::is_same_v<uint16_t, T> || std::is_same_v<int16_t, T>, int> = 0>
  T get_value() {
    _receive_if_necessary(sizeof(T));
    T network_value = 0;
    std::copy_n(_start_position, sizeof(T), reinterpret_cast<char*>(&network_value));
    _start_position += sizeof(T);
    return ntohs(network_value);
  }

  template <typename T, typename std::enable_if_t<std::is_same_v<uint32_t, T> || std::is_same_v<int32_t, T>, int> = 0>
  T get_value() {
    _receive_if_necessary(sizeof(T));
    T network_value = 0;
    std::copy_n(_start_position, sizeof(T), reinterpret_cast<char*>(&network_value));
    _start_position += sizeof(T);
    return ntohl(network_value);
  }

  template <typename T, typename std::enable_if_t<!std::is_same_v<uint16_t, T> && !std::is_same_v<int16_t, T> &&
                                                      !std::is_same_v<uint32_t, T> && !std::is_same_v<int32_t, T>,
                                                  int> = 0>
  T get_value() {
    _receive_if_necessary(sizeof(T));
    T network_value = 0;
    std::copy_n(_start_position, sizeof(T), reinterpret_cast<char*>(&network_value));
    _start_position += sizeof(T);
    return network_value;
  }

  // String functions
  std::string get_string(const size_t string_length, const bool has_null_terminator = true);
  std::string get_string();

  NetworkMessageType get_message_type();

 private:
  std::shared_ptr<SocketType> _socket;
  void _receive_if_necessary(const size_t bytes_required = 1);
};

// Dedicated buffer for write operations. The ring buffer gets extended by methods for writing different data types.
template <typename SocketType>
class WriteBuffer : public RingBuffer {
 public:
  explicit WriteBuffer(const std::shared_ptr<SocketType> socket) : _socket(socket) {}

  // Flush whole buffer, e. g. after a finished request
  void flush(const size_t bytes_required = 0);

  // Put string into the buffer. If the string is longer than the buffer itself the buffer will flush automatically.
  void put_string(const std::string& value, const bool terminate = true);

  // Put numerical values into the buffer. Values will be converted into the correct byte order if necessary.
  template <typename T, typename std::enable_if_t<std::is_same_v<uint32_t, T> || std::is_same_v<int32_t, T>, int> = 0>
  void put_value(const T network_value) {
    _flush_if_necessary(sizeof(T));
    const auto converted_value = htonl(network_value);
    std::copy_n(reinterpret_cast<const char*>(&converted_value), sizeof(T), _current_position);
    _current_position += sizeof(T);
  }

  template <typename T, typename std::enable_if_t<std::is_same_v<uint16_t, T> || std::is_same_v<int16_t, T>, int> = 0>
  void put_value(const T network_value) {
    _flush_if_necessary(sizeof(T));
    const auto converted_value = htons(network_value);
    std::copy_n(reinterpret_cast<const char*>(&converted_value), sizeof(T), _current_position);
    _current_position += sizeof(T);
  }

  template <typename T, typename std::enable_if_t<!std::is_same_v<uint16_t, T> && !std::is_same_v<int16_t, T> &&
                                                      !std::is_same_v<uint32_t, T> && !std::is_same_v<int32_t, T>,
                                                  int> = 0>
  void put_value(const T network_value) {
    _flush_if_necessary(sizeof(T));
    std::copy_n(reinterpret_cast<const char*>(&network_value), sizeof(T), _current_position);
    _current_position += sizeof(T);
  }

 private:
  std::shared_ptr<SocketType> _socket;
  void _flush_if_necessary(const size_t bytes_required);
};

}  // namespace opossum
