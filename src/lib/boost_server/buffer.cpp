#include "buffer.hpp"

#include <arpa/inet.h>

namespace opossum {

static constexpr auto LENGTH_FIELD_SIZE = sizeof(uint32_t);
static constexpr auto MESSAGE_TYPE_SIZE = sizeof(NetworkMessageType);

std::string ReadBuffer::get_string(const size_t string_length) {
  std::string query = "";
  query.reserve(string_length);

  // First, use bytes available in buffer
  if (size() != 0) {
    query = {_start_position, std::min(string_length - 1, size())};
    _start_position += query.size();
  }

  if (string_length == query.size()) {
    return query;
  }

  // Read from network device until string is complete. Ignore last character since it is \0
  while (query.size() < string_length - 1) {
    const auto substring_length = std::min(string_length - query.size(), BUFFER_SIZE) - 1;
    _receive_if_necessary(substring_length);
    std::copy_n(_start_position, substring_length, std::back_inserter(query));
    _start_position += substring_length;
  }

  // Skip ignored string terminator
  _start_position++;

  return query;
}

NetworkMessageType ReadBuffer::get_message_type() {
  _receive_if_necessary(MESSAGE_TYPE_SIZE);
  auto message_type = static_cast<NetworkMessageType>(*_start_position);
  _start_position++;
  return message_type;
}

void ReadBuffer::_receive_if_necessary(const size_t bytes_required) {
  while (_unprocessed_bytes() < bytes_required) {
    _receive();
  }
}

void ReadBuffer::_receive() {
  // TODO(toni): Check if enough space BUFFER_SIZE
  if (_start_position == _current_position) reset();
  const auto bytes_read = _socket->read_some(boost::asio::buffer(_current_position, BUFFER_SIZE));
  // TODO(toni): handle bytes_read == 0 -> terminate
  _current_position += bytes_read;
}

void WriteBuffer::put_string(const std::string& value, const bool terminate) {
  auto position_in_string = 0u;

  // Use available space first
  if (!full()) {
    position_in_string = std::min(BUFFER_SIZE - size() - 1, value.size());
    std::copy_n(value.begin(), position_in_string, _current_position);
    _current_position += position_in_string;
  }

  // Read from network device until string is complete. Ignore last character since it is \0
  while (position_in_string < value.size()) {
    const auto bytes_to_transfer = std::min(BUFFER_SIZE - 1, value.size() - position_in_string);
    _flush_if_necessary(bytes_to_transfer);
    std::copy_n(value.begin() + position_in_string, bytes_to_transfer, _current_position);
    _current_position += bytes_to_transfer;
    position_in_string += bytes_to_transfer;
  }

  // Add string terminator if necessary
  if (terminate) {
    _flush_if_necessary(1u);
    *_current_position = '\0';
    _current_position++;
  }
}

void WriteBuffer::_flush_if_necessary(const size_t bytes_required) {
  if (bytes_required > BUFFER_SIZE - size() - 1) {
    _send();
  }
}

void WriteBuffer::flush() { _send(); }

void WriteBuffer::_send() {
  [[maybe_unused]] const auto bytes_sent = _socket->write_some(boost::asio::buffer(_start_position, size()));
  // TODO(toni): handle bytes_sent == 0 -> terminate
  reset();
}
}  // namespace opossum
