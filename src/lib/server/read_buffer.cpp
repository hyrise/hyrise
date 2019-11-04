#include "read_buffer.hpp"

#include "client_disconnect_exception.hpp"

namespace opossum {

template <typename SocketType>
size_t ReadBuffer<SocketType>::size() const {
  const auto current_size = std::distance(&*_start_position, &*_current_position);
  return current_size < 0 ? current_size + SERVER_BUFFER_SIZE : current_size;
}

template <typename SocketType>
size_t ReadBuffer<SocketType>::maximum_capacity() const {
  return SERVER_BUFFER_SIZE - 1;
}

template <typename SocketType>
bool ReadBuffer<SocketType>::full() const {
  return size() == maximum_capacity();
}

template <typename SocketType>
std::string ReadBuffer<SocketType>::get_string() {
  auto string_end = RingBufferIterator(_data);
  std::string result;

  // First, use bytes available in buffer
  if (size() != 0) {
    string_end = std::find(_start_position, _current_position, '\0');
    std::copy(_start_position, string_end, std::back_inserter(result));
    std::advance(_start_position, result.size());
  }

  while (*string_end != '\0') {
    // We do not know how long this string is going to be. Hence, receive at least 1 char.
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
                                               const HasNullTerminator has_null_terminator) {
  std::string result;
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
  if (has_null_terminator == HasNullTerminator::Yes) {
    Assert(result.back() == '\0', "Last character is not a null terminator");
    result.pop_back();
  }

  return result;
}

template <typename SocketType>
void ReadBuffer<SocketType>::_receive_if_necessary(const size_t bytes_required) {
  // Already enough data present in buffer
  if (size() >= bytes_required) {
    return;
  }

  // Buffer might contain unread data, so cannot read full buffer size
  const auto maximum_readable_size = maximum_capacity() - size();

  size_t bytes_read;
  boost::system::error_code error_code;
  // We cannot forward an iterator to the read system call. Hence, we need to use raw pointers. Therefore, we need to
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
    throw ClientDisconnectException("Read operation failed. Client closed connection.");
  }

  Assert(!error_code, error_code.message());

  std::advance(_current_position, bytes_read);
}

template class ReadBuffer<Socket>;
template class ReadBuffer<boost::asio::posix::stream_descriptor>;

}  // namespace opossum
