#include "client_connection.hpp"

#include "use_boost_future.hpp"
#include "postgres_wire_handler.hpp"

namespace opossum {

boost::future<uint32_t> ClientConnection::receive_startup_package_header() {
  static const uint32_t STARTUP_HEADER_LENGTH = 8u;

  return receive_bytes_async(STARTUP_HEADER_LENGTH)
    .then([](boost::future<InputPacket> packet) {
      return PostgresWireHandler::handle_startup_package(packet.get());
    });
}

boost::future<void> ClientConnection::receive_startup_package_contents(uint32_t size) {
  return receive_bytes_async(size)
    .then([](boost::future<InputPacket> packet) {
      auto p = packet.get();
      // Read these values and ignore them
      PostgresWireHandler::handle_startup_package_content(p, p.data.size());
    });
}

boost::future<void> ClientConnection::send_ssl_denied() {
  // Don't use new_output_packet here, because this packet has special size requirements (only contains N, no size)
  OutputPacket output_packet;
  PostgresWireHandler::write_value(output_packet, NetworkMessageType::SslNo);
  
  return send_bytes_async(output_packet)
    .then([=](boost::future<unsigned long> f) { f.get(); return flush_async(); }).unwrap()
    .then([](boost::future<unsigned long> f){ f.get(); });
}

boost::future<void> ClientConnection::send_auth() {
  OutputPacket output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::AuthenticationRequest);
  PostgresWireHandler::write_value(output_packet, htonl(0u));

  return send_bytes_async(output_packet).then([](boost::future<unsigned long> f){ f.get(); });
}

boost::future<void> ClientConnection::send_ready_for_query() {
  // ReadyForQuery packet 'Z' with transaction status Idle 'I'
  OutputPacket output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ReadyForQuery);
  PostgresWireHandler::write_value(output_packet, TransactionStatusIndicator::Idle);

  return send_bytes_async(output_packet)
    .then([=](boost::future<unsigned long> f) { f.get(); return flush_async(); }).unwrap()
    .then([](boost::future<unsigned long> f){ f.get(); });
}

boost::future<InputPacket> ClientConnection::receive_bytes_async(size_t size) {
  InputPacket result;
  return _socket.async_read_some(boost::asio::buffer(result.data, size), boost::asio::use_boost_future)
    .then(boost::launch::executor, [&](boost::future<unsigned long> f) {
      f.get();  // Throws any errors
      result.offset = result.data.begin();
      return result;
    });
}

boost::future<unsigned long> ClientConnection::send_bytes_async(OutputPacket& packet) {
  // If the packet is SslNo (size == 1), it has a special format and does not require a size
  if (packet.data.size() > 1) {
    PostgresWireHandler::write_output_packet_size(packet);
  }

  if (_response_buffer.size() + packet.data.size() > _max_response_size) {
    return flush_async();
  }

  _response_buffer.insert(_response_buffer.end(), packet.data.begin(), packet.data.end());
  
  // Return an already resolved future (we have just written data to the buffer)
  boost::promise<unsigned long> result;
  result.set_value(_response_buffer.size());
  return result.get_future();
}

boost::future<unsigned long> ClientConnection::flush_async() {
  return _socket.async_write_some(boost::asio::buffer(_response_buffer), boost::asio::use_boost_future);
}


} // namespace opossum