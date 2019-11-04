#include "write_buffer.hpp"

#include "client_disconnect_exception.hpp"

namespace opossum {

template <typename SocketType>
size_t WriteBuffer<SocketType>::size() const {
  const auto current_size = std::distance(&*_start_position, &*_current_position);
  return current_size < 0 ? current_size + SERVER_BUFFER_SIZE : current_size;
}

template <typename SocketType>
size_t WriteBuffer<SocketType>::maximum_capacity() const {
  return SERVER_BUFFER_SIZE - 1;
}

template <typename SocketType>
bool WriteBuffer<SocketType>::full() const {
  return size() == maximum_capacity();
}

template <typename SocketType>
void WriteBuffer<SocketType>::put_string(const std::string& value, const HasNullTerminator has_null_terminator) {
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
  if (has_null_terminator == HasNullTerminator::Yes) {
    _flush_if_necessary(sizeof(char));
    *_current_position = '\0';
    _current_position++;
  }
}

template <typename SocketType>
void WriteBuffer<SocketType>::flush(const size_t bytes_required) {
  Assert(bytes_required <= size(), "Cannot flush more byte than available");
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
    throw ClientDisconnectException("Write operation failed. Client closed connection.");
  }
  Assert(!error_code, error_code.message());

  std::advance(_start_position, bytes_sent);
}

template <typename SocketType>
void WriteBuffer<SocketType>::_flush_if_necessary(const size_t bytes_required) {
  if (bytes_required >= maximum_capacity() - size()) {
    flush(bytes_required);
  }
}

template class WriteBuffer<Socket>;
template class WriteBuffer<boost::asio::posix::stream_descriptor>;

}  // namespace opossum
