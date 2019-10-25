#pragma once

#include <boost/asio.hpp>

#include "ring_buffer_iterator.hpp"
#include "types.hpp"

namespace opossum {

using Socket = boost::asio::ip::tcp::socket;

// Dedicated buffer for read operations. The ring buffer gets extended by methods for reading different data types.
template <typename SocketType>
class ReadBuffer {
 public:
  explicit ReadBuffer(std::shared_ptr<SocketType> socket) : _socket(socket) {}

  // Problem: full and empty might be same state, so head == tail
  // Solution: Full state is tail + 1 == head
  //           Empty state is head == tail
  size_t size() const;

  // See comment above
  size_t maximum_capacity() const;

  // Check if buffer is full
  bool full() const;

  // Extract numerical values from buffer. Values will be converted into the correct byte order if necessary.
  template <typename T>
  T get_value() {
    _receive_if_necessary(sizeof(T));
    T network_value = 0;
    std::copy_n(_start_position, sizeof(T), reinterpret_cast<char*>(&network_value));
    std::advance(_start_position, sizeof(T));
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

 private:
  void _receive_if_necessary(const size_t bytes_required = 1);

  std::array<char, BUFFER_SIZE> _data;
  RingBufferIterator _start_position{_data};
  RingBufferIterator _current_position{_data};
  std::shared_ptr<SocketType> _socket;
};

}  // namespace opossum
