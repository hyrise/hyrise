#include "ring_buffer.hpp"

#include <pthread.h>
#include <iostream>

namespace opossum {

std::string ReadBuffer::get_string() {
  auto string_end = RingBufferIterator(_data);
  std::string result = "";

  // First, use bytes available in buffer
  if (size() != 0) {
    string_end = std::find(_start_position, _current_position, '\0');
    std::copy(_start_position, string_end, std::back_inserter(result));
    _start_position += result.size();
  }

  // String might already be complete at this point
  if (*string_end == '\0') {
    // Skip null terminator
    _start_position++;
    return result;
  }

  while (*string_end != '\0') {
    // We dont know how long this string is going to be. Hence, receive at least 1 char.
    _receive_if_necessary();
    string_end = std::find(_start_position, _current_position, '\0');
    std::copy(_start_position, string_end, std::back_inserter(result));
    _start_position = string_end;
  }
  // Skip null terminator
  _start_position++;

  return result;
}

std::string ReadBuffer::get_string(const size_t string_length, const bool has_null_terminator) {
  std::string result = "";
  result.reserve(string_length);

  // First, use bytes available in buffer
  if (size() != 0) {
    std::copy_n(_start_position, std::min(string_length, size()), std::back_inserter(result));
    _start_position += result.size();
  }

  // Read from network device until string is complete or skip if string is already complete.
  while (result.size() < string_length) {
    const auto substring_length = std::min(string_length - result.size(), maximum_capacity());
    _receive_if_necessary(substring_length);
    std::copy_n(_start_position, substring_length, std::back_inserter(result));
    _start_position += substring_length;
  }

  // Ignore last character if it is \0
  if (has_null_terminator) {
    result.pop_back();
  }

  return result;
}

NetworkMessageType ReadBuffer::get_message_type() {
  _receive_if_necessary();
  auto message_type = static_cast<NetworkMessageType>(*_start_position);
  _start_position++;
  return message_type;
}

void ReadBuffer::_receive_if_necessary(const size_t bytes_required) {
  // Already enough data present in buffer
  if (size() >= bytes_required) {
    return;
  }

  // Buffer might contain unread data, so cant read full buffer size
  const auto maximum_readable_size = maximum_capacity() - size();

  size_t bytes_read;
  boost::system::error_code error_code;
  // We can't forward an iterator to the read system call. Hence, we need to use raw pointers. Therefore, we need to
  // distinguish between reading into continuous memory or partially read the data.
  if ((_current_position - _start_position) < 0 || _start_position.get_raw_pointer() == &_data[0]) {
    bytes_read =
        boost::asio::read(*_socket, boost::asio::buffer(_current_position.get_raw_pointer(), maximum_readable_size),
                          boost::asio::transfer_at_least(bytes_required - size()), error_code);
  } else {
    bytes_read = boost::asio::read(
        *_socket,
        std::array<boost::asio::mutable_buffer, 2>{
            boost::asio::buffer(_current_position.get_raw_pointer(),
                                std::distance(_current_position.get_raw_pointer(), _data.end())),
            boost::asio::buffer(_data.begin(), std::distance(_data.begin(), _start_position.get_raw_pointer() - 1))},
        boost::asio::transfer_at_least(bytes_required - size()), error_code);
  }

  // Socket was closed by client during execution
  if (error_code == boost::asio::error::broken_pipe || error_code == boost::asio::error::connection_reset) {
    // Terminate session by stopping the current thread
    pthread_exit(nullptr);
  } else if (error_code) {
    std::cerr << error_code.category().name() << ": " << error_code.message() << std::endl;
  }
  _current_position += bytes_read;
}

void WriteBuffer::put_string(const std::string& value, const bool terminate) {
  auto position_in_string = 0u;

  // Use available space first
  if (!full()) {
    position_in_string = static_cast<uint32_t>(std::min(maximum_capacity() - size(), value.size()));
    std::copy_n(value.cbegin(), position_in_string, _current_position);
    _current_position += position_in_string;
  }

  // Read from network device until string is complete. Ignore last character since it is \0
  while (position_in_string < value.size()) {
    const auto bytes_to_transfer = std::min(maximum_capacity(), value.size() - position_in_string);
    _flush_if_necessary(bytes_to_transfer);
    std::copy_n(value.cbegin() + position_in_string, bytes_to_transfer, _current_position);
    _current_position += bytes_to_transfer;
    position_in_string += bytes_to_transfer;
  }

  // Add string terminator if necessary
  if (terminate) {
    _flush_if_necessary(sizeof(char));
    *_current_position = '\0';
    _current_position++;
  }
}

void WriteBuffer::flush(const size_t bytes_required) {
  const auto bytes_to_send = bytes_required ? bytes_required : size();
  size_t bytes_sent;

  boost::system::error_code error_code;
  if ((_current_position - _start_position) < 0) {
    // Data not continously stored in buffer
    bytes_sent = boost::asio::write(
        *_socket,
        std::array<boost::asio::mutable_buffer, 2>{
            boost::asio::buffer(_start_position.get_raw_pointer(),
                                std::distance(_start_position.get_raw_pointer(), _data.end())),
            boost::asio::buffer(_data.begin(), std::distance(_data.begin(), _current_position.get_raw_pointer()))},
        boost::asio::transfer_at_least(bytes_to_send), error_code);
  } else {
    bytes_sent = boost::asio::write(*_socket, boost::asio::buffer(_start_position.get_raw_pointer(), size()),
                                    boost::asio::transfer_at_least(bytes_to_send), error_code);
  }

  // Socket was closed by client during execution
  if (error_code == boost::asio::error::broken_pipe || error_code == boost::asio::error::connection_reset) {
    // Terminate session by stopping the current thread
    pthread_exit(nullptr);
  } else if (error_code) {
    std::cerr << error_code.category().name() << ": " << error_code.message() << std::endl;
  }
  _start_position += bytes_sent;
}

void WriteBuffer::_flush_if_necessary(const size_t bytes_required) {
  if (bytes_required >= maximum_capacity() - size()) {
    flush(bytes_required);
  }
}
}  // namespace opossum
