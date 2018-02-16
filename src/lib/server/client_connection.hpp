#pragma once

#include <memory>

#include <boost/asio/ip/tcp.hpp>
#include <boost/thread/future.hpp>

namespace opossum {

using boost::asio::ip::tcp;

using ByteBuffer = std::vector<char>;
struct InputPacket;
struct OutputPacket;

class ClientConnection {
 public:
  explicit ClientConnection(tcp::socket socket) : _socket(std::move(socket)) {}
  // TODO: Close socket on destruction

  boost::future<uint32_t> receive_startup_package_header();
  boost::future<void> receive_startup_package_contents(uint32_t size);
  

  boost::future<void> send_ssl_denied();
  boost::future<void> send_auth();
  boost::future<void> send_ready_for_query();

 protected:
  boost::future<InputPacket> receive_bytes_async(size_t size);
  boost::future<unsigned long> send_bytes_async(OutputPacket &packet);
  boost::future<unsigned long> flush_async();

  tcp::socket _socket;
  
  // Max 2048 bytes per IP packet sent
  uint32_t _max_response_size = 2048;
  ByteBuffer _response_buffer;
};

} // namespace opossum