#pragma once

#include <array>
#include <algorithm>
#include <boost/asio.hpp>
#include <cstdint>
#include <cstring>
#include <string>
#include "network_message_types.hpp"

namespace opossum {

using Socket = boost::asio::ip::tcp::socket;

static constexpr size_t BUFFER_SIZE = 1024u;

class Buffer {
 public:
  explicit Buffer(std::shared_ptr<Socket> socket) : _socket(socket) {}

  inline const char* data() const{
    return _data.begin();
  }

  inline size_t size() const {
    return _current_position - _data.begin();
  }

  inline void reset() {
    _start_position = _data.begin();
    _current_position = _data.begin();
  }

  inline bool full() {
    return size() == (BUFFER_SIZE - 1);
  }

 protected:
  inline size_t _unprocessed_bytes() const {
    return std::distance(_start_position, _current_position);
  }

  std::shared_ptr<Socket> _socket;
  std::array<char, BUFFER_SIZE> _data;
  std::array<char, BUFFER_SIZE>::iterator _start_position = _data.begin();
  std::array<char, BUFFER_SIZE>::iterator _current_position = _data.begin();
};


class ReadBuffer : public Buffer {
public:
  explicit ReadBuffer(std::shared_ptr<Socket> socket) : Buffer(socket) {}

  template <typename T>
  T get_value() {
    _receive_if_necessary(sizeof(T));
    T network_value;
    // TODO ntohl here?
    std::memcpy(&network_value, _start_position, sizeof(T));
    _start_position += sizeof(T);
    return network_value;
  }

  std::string get_string(const size_t string_length);

  // TODO(toni): remove?
  NetworkMessageType get_message_type();

 protected:
  void _receive_if_necessary(const size_t bytes_required);

  void _receive();
};


class WriteBuffer : public Buffer {
public:
  explicit WriteBuffer(std::shared_ptr<Socket> socket) : Buffer(socket) {}

  void flush();

  void put_string(const std::string& value, const bool terminate = true);

  template <typename T>
  void put_value(const T network_value) {
    _flush_if_necessary(sizeof(T));
    std::memcpy(_current_position, reinterpret_cast<const char*>(&network_value), sizeof(T));
    _current_position += sizeof(T);
  }

 protected:
  void _flush_if_necessary(const size_t bytes_required);

  void _send();
};
}  // namespace opossum
