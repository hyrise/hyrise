#pragma once

#include <array>
#include <boost/asio.hpp>
#include <iterator>
#include "network_message_types.hpp"

namespace opossum {

using Socket = boost::asio::ip::tcp::socket;

static constexpr size_t BUFFER_SIZE = 4096u;

class BufferIterator : public std::iterator<std::forward_iterator_tag, char> {
 public:
  explicit BufferIterator(std::array<char, BUFFER_SIZE>& data, size_t position = 0)
      : _data(data), _position(position) {}

  BufferIterator(const BufferIterator&) = default;

  BufferIterator& operator=(const BufferIterator& other) {
    _position = other._position;
    _data = other._data;
    return *this;
  }

  bool operator==(BufferIterator other) const { return _position == other._position; }

  bool operator!=(BufferIterator other) const { return !(*this == other); }

  BufferIterator& operator++() {
    _position = (_position + 1) % BUFFER_SIZE;
    return *this;
  }

  BufferIterator operator++(int) {
    BufferIterator retval = *this;
    ++(*this);
    return retval;
  }

  BufferIterator& operator+(const size_t increment) {
    _position = (_position + increment) % BUFFER_SIZE;
    return *this;
  };

  BufferIterator& operator+=(const size_t increment) { return operator+(increment); }

  BufferIterator::difference_type operator-(BufferIterator const& other) const { return _position - other._position; }

  reference operator*() const { return _data[_position]; }

 private:
  std::array<char, BUFFER_SIZE>& _data;
  size_t _position;
};

class Buffer {
 public:
  explicit Buffer(const std::shared_ptr<Socket> socket) : _socket(socket) {}

  // really inline?
  inline char* data() noexcept { return _data.begin(); }

  // Problem: full and empty might be same state, so head == tail
  // Solution: Full state is tail + 1 == head
  //           Empty state is head == tail
  inline size_t size() const {
    const auto current_size = _current_position - _start_position;
    if (current_size < 0) {
      return current_size + BUFFER_SIZE;
    } else {
      return current_size;
    }
  }

  inline void reset() { _start_position = _current_position; }

  inline bool full() { return size() == (BUFFER_SIZE - 1); }

 protected:
  std::shared_ptr<Socket> _socket;
  std::array<char, BUFFER_SIZE> _data;
  BufferIterator _start_position = BufferIterator(_data);
  BufferIterator _current_position = BufferIterator(_data);
};

class ReadBuffer : public Buffer {
 public:
  explicit ReadBuffer(std::shared_ptr<Socket> socket) : Buffer(socket) {}

  template <typename T, typename std::enable_if_t<std::is_same_v<uint16_t, T> || std::is_same_v<int16_t, T>, int> = 0>
  T get_value() {
    _receive_if_necessary(sizeof(T));
    T network_value;
    std::copy_n(_start_position, sizeof(T), reinterpret_cast<char*>(&network_value));
    _start_position += sizeof(T);
    return ntohs(network_value);
  }

  template <typename T, typename std::enable_if_t<std::is_same_v<uint32_t, T> || std::is_same_v<int32_t, T>, int> = 0>
  T get_value() {
    _receive_if_necessary(sizeof(T));
    T network_value;
    std::copy_n(_start_position, sizeof(T), reinterpret_cast<char*>(&network_value));
    _start_position += sizeof(T);
    return ntohl(network_value);
  }

  template <typename T, typename std::enable_if_t<!std::is_same_v<uint16_t, T> && !std::is_same_v<int16_t, T> &&
                                                      !std::is_same_v<uint32_t, T> && !std::is_same_v<int32_t, T>,
                                                  int> = 0>
  T get_value() {
    _receive_if_necessary(sizeof(T));
    T network_value;
    std::copy_n(_start_position, sizeof(T), reinterpret_cast<char*>(&network_value));
    _start_position += sizeof(T);
    return network_value;
  }

  std::string get_string(const size_t string_length, const bool has_null_terminator = true);
  std::string get_string();

  NetworkMessageType get_message_type();

 protected:
  void _receive_if_necessary(const size_t bytes_required = 1);
};

class WriteBuffer : public Buffer {
 public:
  explicit WriteBuffer(const std::shared_ptr<Socket> socket) : Buffer(socket) {}

  void flush(const size_t bytes_required = 0);

  void put_string(const std::string& value, const bool terminate = true);

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

 protected:
  void _flush_if_necessary(const size_t bytes_required);
};
}  // namespace opossum
