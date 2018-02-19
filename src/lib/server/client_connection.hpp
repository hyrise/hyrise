#pragma once

#include <memory>

#include <boost/asio/ip/tcp.hpp>
#include <boost/thread/future.hpp>

namespace opossum {

using boost::asio::ip::tcp;

using ByteBuffer = std::vector<char>;
struct InputPacket;
struct OutputPacket;
struct RequestHeader;
enum class NetworkMessageType : unsigned char;

struct ColumnDescription {
  std::string column_name;
  uint64_t object_id;
  int64_t type_width;
};

class ClientConnection {
  friend class SendQueryResponseTask;

 public:
  explicit ClientConnection(tcp::socket socket) : _socket(std::move(socket)) {
    _response_buffer.reserve(_max_response_size);
  }

  boost::future<uint32_t> receive_startup_packet_header();
  boost::future<void> receive_startup_packet_contents(uint32_t size);

  boost::future<RequestHeader> receive_packet_header();
  boost::future<InputPacket> receive_packet_contents(uint32_t size);

  boost::future<void> send_ssl_denied();
  boost::future<void> send_auth();
  boost::future<void> send_ready_for_query();
  boost::future<void> send_error(const std::string& message);
  boost::future<void> send_notice(const std::string& notice);
  boost::future<void> send_status_message(const NetworkMessageType& type);
  boost::future<void> send_row_description(const std::vector<ColumnDescription> row_description);
  boost::future<void> send_command_complete(const std::string& message);

 protected:
  boost::future<InputPacket> _receive_bytes_async(size_t size);

  boost::future<uint64_t> _send_bytes_async(std::shared_ptr<OutputPacket> packet, bool flush = false);
  boost::future<uint64_t> _flush_async();

  tcp::socket _socket;

  // Max 2048 bytes per IP packet sent
  uint32_t _max_response_size = 2048;
  ByteBuffer _response_buffer;
};

}  // namespace opossum