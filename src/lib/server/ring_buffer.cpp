#include "ring_buffer.hpp"

#include <pthread.h>
#include <iostream>

namespace opossum {

template <typename SocketType>
std::string ReadBuffer<SocketType>::get_string() {
  auto string_end = RingBufferIterator(_data);
  std::string result = "";

  // First, use bytes available in buffer
  if (size() != 0) {
    string_end = std::find(_start_position, _current_position, '\0');
    std::copy(_start_position, string_end, std::back_inserter(result));
    std::advance(_start_position, result.size());
  }

  // String might already be complete at this point
  if (*string_end == '\0') {
    // Skip null terminator
    _start_position++;
    return result;
  }

  while (*string_end != '\0') {
    // We don't know how long this string is going to be. Hence, receive at least 1 char.
    _receive_if_necessary();
    string_end = std::find(_start_position, _current_position, '\0');
    std::copy(_start_position, string_end, std::back_inserter(result));
    _start_position = string_end;
  }
  // Skip null terminator
  _start_position++;

  return result;
}

template <typename SocketType>
std::string ReadBuffer<SocketType>::get_string(const size_t string_length,
                                               const IgnoreNullTerminator ignore_null_terminator) {
  std::string result = "";
  result.reserve(string_length);

  // First, use bytes available in buffer
  if (size() != 0) {
    std::copy_n(_start_position, std::min(string_length, size()), std::back_inserter(result));
    std::advance(_start_position, result.size());
  }

  // Read from network device until string is complete or skip if string is already complete.
  while (result.size() < string_length) {
    const auto substring_length = std::min(string_length - result.size(), maximum_capacity());
    _receive_if_necessary(substring_length);
    std::copy_n(_start_position, substring_length, std::back_inserter(result));
    std::advance(_start_position, substring_length);
  }

  // Ignore last character if it is \0
  if (ignore_null_terminator == IgnoreNullTerminator::No) {
    result.pop_back();
  }

  return result;
}

template <typename SocketType>
PostgresMessageType ReadBuffer<SocketType>::get_message_type() {
  _receive_if_necessary();
  auto message_type = static_cast<PostgresMessageType>(*_start_position);
  _start_position++;
  return message_type;
}

template <typename SocketType>
void ReadBuffer<SocketType>::_receive_if_necessary(const size_t bytes_required) {
  // Already enough data present in buffer
  if (size() >= bytes_required) {
    return;
  }

  // Buffer might contain unread data, so can't read full buffer size
  const auto maximum_readable_size = maximum_capacity() - size();

  size_t bytes_read;
  boost::system::error_code error_code;
  // We can't forward an iterator to the read system call. Hence, we need to use raw pointers. Therefore, we need to
  // distinguish between reading into continuous memory or partially read the data.
  if (std::distance(&*_start_position, &*_current_position) < 0 || &*_start_position == &_data[0]) {
    bytes_read = boost::asio::read(*_socket, boost::asio::buffer(&*_current_position, maximum_readable_size),
                                   boost::asio::transfer_at_least(bytes_required - size()), error_code);
  } else {
    bytes_read =
        boost::asio::read(*_socket,
                          std::array<boost::asio::mutable_buffer, 2>{
                              boost::asio::buffer(&*_current_position, std::distance(&*_current_position, _data.end())),
                              boost::asio::buffer(_data.begin(), std::distance(_data.begin(), &*_start_position - 1))},
                          boost::asio::transfer_at_least(bytes_required - size()), error_code);
  }

  // Socket was closed by client during execution
  if (error_code == boost::asio::error::broken_pipe || error_code == boost::asio::error::connection_reset ||
      bytes_read == 0) {
    // Terminate session by stopping the current thread
    pthread_exit(nullptr);
  } else if (error_code) {
    std::cerr << error_code.category().name() << ": " << error_code.message() << std::endl;
  }
  std::advance(_current_position, bytes_read);
}

template <typename SocketType>
void WriteBuffer<SocketType>::put_string(const std::string& value, const IgnoreNullTerminator ignore_null_terminator) {
  auto position_in_string = 0u;

  // Use available space first
  if (!full()) {
    position_in_string = static_cast<uint32_t>(std::min(maximum_capacity() - size(), value.size()));
    std::copy_n(value.cbegin(), position_in_string, _current_position);
    std::advance(_current_position, position_in_string);
  }

  // Write to network device until string is complete. Ignore last character since it is \0
  while (position_in_string < value.size()) {
    const auto bytes_to_transfer = std::min(maximum_capacity(), value.size() - position_in_string);
    _flush_if_necessary(bytes_to_transfer);
    std::copy_n(value.cbegin() + position_in_string, bytes_to_transfer, _current_position);
    std::advance(_current_position, bytes_to_transfer);
    position_in_string += bytes_to_transfer;
  }

  // Add string terminator if necessary
  if (ignore_null_terminator == IgnoreNullTerminator::No) {
    _flush_if_necessary(sizeof(char));
    *_current_position = '\0';
    _current_position++;
  }
}

template <typename SocketType>
void WriteBuffer<SocketType>::flush(const size_t bytes_required) {
  const auto bytes_to_send = bytes_required ? bytes_required : size();
  size_t bytes_sent;

  boost::system::error_code error_code;
  if (std::distance(&*_start_position, &*_current_position) < 0) {
    // Data not continuously stored in buffer
    bytes_sent =
        boost::asio::write(*_socket,
                           std::array<boost::asio::mutable_buffer, 2>{
                               boost::asio::buffer(&*_start_position, std::distance(&*_start_position, _data.end())),
                               boost::asio::buffer(_data.begin(), std::distance(_data.begin(), &*_current_position))},
                           boost::asio::transfer_at_least(bytes_to_send), error_code);
  } else {
    bytes_sent = boost::asio::write(*_socket, boost::asio::buffer(&*_start_position, size()),
                                    boost::asio::transfer_at_least(bytes_to_send), error_code);
  }

  // Socket was closed by client during execution
  if (error_code == boost::asio::error::broken_pipe || error_code == boost::asio::error::connection_reset ||
      bytes_sent == 0) {
    // Terminate session by stopping the current thread
    pthread_exit(nullptr);
  } else if (error_code) {
    std::cerr << error_code.category().name() << ": " << error_code.message() << std::endl;
  }
  std::advance(_start_position, bytes_sent);
}

template <typename SocketType>
void WriteBuffer<SocketType>::_flush_if_necessary(const size_t bytes_required) {
  if (bytes_required >= maximum_capacity() - size()) {
    flush(bytes_required);
  }
}

template class ReadBuffer<Socket>;
template class ReadBuffer<boost::asio::posix::stream_descriptor>;
template class WriteBuffer<Socket>;
template class WriteBuffer<boost::asio::posix::stream_descriptor>;

}  // namespace opossum
