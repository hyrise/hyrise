#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/thread/future.hpp>

#include <memory>

namespace opossum {

using ByteBuffer = std::vector<char>;
struct InputPacket;
struct OutputPacket;
struct RequestHeader;
struct ParsePacket;
struct BindPacket;
enum class NetworkMessageType : unsigned char;

struct ColumnDescription {
  std::string column_name;
  uint64_t object_id;
  int64_t type_width;
};

// This class provides a wrapper over the TCP socket and (de)serializes
// network messages using the PostgresWireHandler. It's a very thin wrapper
// because the ASIO socket is hard to mock, so there are no tests for this class
class ClientConnection : public std::enable_shared_from_this<ClientConnection> {
 public:
  explicit ClientConnection(boost::asio::ip::tcp::socket socket);

  boost::future<uint32_t> receive_startup_packet_header();
  boost::future<void> receive_startup_packet_body(uint32_t size);

  boost::future<RequestHeader> receive_packet_header();
  boost::future<std::string> receive_simple_query_packet_body(uint32_t size);
  boost::future<ParsePacket> receive_parse_packet_body(uint32_t size);
  boost::future<BindPacket> receive_bind_packet_body(uint32_t size);
  boost::future<std::string> receive_describe_packet_body(uint32_t size);
  boost::future<void> receive_sync_packet_body(uint32_t size);
  boost::future<void> receive_flush_packet_body(uint32_t size);
  boost::future<std::string> receive_execute_packet_body(uint32_t size);

  boost::future<void> send_ssl_denied();
  boost::future<void> send_auth();
  boost::future<void> send_parameter_status(const std::string& key, const std::string& value);
  boost::future<void> send_ready_for_query();
  boost::future<void> send_error(const std::string& message);
  boost::future<void> send_notice(const std::string& notice);
  boost::future<void> send_status_message(const NetworkMessageType& type);
  boost::future<void> send_row_description(const std::vector<ColumnDescription>& row_description);
  boost::future<void> send_data_row(const std::vector<std::string>& row_strings);
  boost::future<void> send_command_complete(const std::string& message);

 protected:
  boost::future<InputPacket> _receive_bytes_async(size_t size);

  boost::future<uint64_t> _send_bytes_async(const std::shared_ptr<OutputPacket>& packet, bool flush = false);
  boost::future<uint64_t> _flush_async();

  boost::asio::ip::tcp::socket _socket;

  // Max 2048 bytes per IP packet sent
  uint32_t _max_response_size = 2048;
  ByteBuffer _response_buffer;
};

}  // namespace opossum
