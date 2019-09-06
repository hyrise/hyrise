#include "buffer.hpp"

namespace opossum {

static constexpr auto LENGTH_FIELD_SIZE = sizeof(uint32_t);
static constexpr auto MESSAGE_TYPE_SIZE = sizeof(NetworkMessageType);

std::string ReadBuffer::get_string() {
  auto string_end = BufferIterator(_data);
  std::string result = "";

  // First, use bytes available in buffer
  if (size() != 0) {
    // We have to convert the byte buffer into a std::string, making sure
    // we don't run past the end of the buffer and stop at the first null byte
    string_end = std::find(_start_position, _current_position, '\0');
    std::copy(_start_position, string_end, std::back_inserter(result));
    // Off by 1?
    _start_position += result.size();
  }

  // String might already be complete at this point
  if (*string_end == '\0') {
    // Skip null terminator
    _start_position++;
    return result;
  }

  while (*string_end != '\0') {
    // We dont know how long this is going to be. Hence, receive at least 1 char.
    _receive_if_necessary(1u);
    string_end = std::find(_start_position, _current_position, '\0');
    std::copy(_start_position, string_end, std::back_inserter(result));
    _start_position = string_end;
  }
  // Skip null terminator
  _start_position++;

  return result;
}

// TODO(toni): doc: has to include null terminator
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
    const auto substring_length = std::min(string_length - result.size(), BUFFER_SIZE - 1);
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
  _receive_if_necessary(MESSAGE_TYPE_SIZE);
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
  const auto maximum_readable_size = BUFFER_SIZE - size() - 1;

  size_t bytes_read;
  if ((_current_position - _start_position) < 0 || &*_start_position == &_data[0]) {
    bytes_read = boost::asio::read(*_socket, boost::asio::buffer(&(*_current_position), maximum_readable_size),
                                   boost::asio::transfer_at_least(bytes_required - size()));
  } else {
    bytes_read =
        boost::asio::read(*_socket,
                          std::array<boost::asio::mutable_buffer, 2>{
                              boost::asio::buffer(&*_current_position, std::distance(&*_current_position, _data.end())),
                              boost::asio::buffer(_data.begin(), std::distance(_data.begin(), &*_start_position - 1))},
                          boost::asio::transfer_at_least(bytes_required - size()));
  }

  if (!bytes_read) {
    // TODO(toni): Connection abort
  }
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

void WriteBuffer::flush(const size_t bytes_required) {
  const auto bytes_to_send = bytes_required ? bytes_required : size();
  size_t bytes_sent;

  if ((_current_position - _start_position) < 0) {
    // Data not continously stored in buffer
    bytes_sent =
        boost::asio::write(*_socket,
                           std::array<boost::asio::mutable_buffer, 2>{
                               boost::asio::buffer(&*_start_position, std::distance(&*_start_position, _data.end())),
                               boost::asio::buffer(_data.begin(), std::distance(_data.begin(), &*_current_position))},
                           boost::asio::transfer_at_least(bytes_to_send));
  } else {
    bytes_sent = boost::asio::write(*_socket, boost::asio::buffer(&*_start_position, size()),
                                    boost::asio::transfer_at_least(bytes_to_send));
  }

  if (!bytes_sent) {
    // TODO(toni): connection abort
  }
  _start_position += bytes_sent;
}

void WriteBuffer::_flush_if_necessary(const size_t bytes_required) {
  if (bytes_required >= BUFFER_SIZE - size() - 1) {
    flush(bytes_required);
  }
}
}  // namespace opossum
