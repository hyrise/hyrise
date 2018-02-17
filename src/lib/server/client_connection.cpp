#include "client_connection.hpp"

#include "use_boost_future.hpp"
#include "postgres_wire_handler.hpp"

namespace opossum {

boost::future<uint32_t> ClientConnection::receive_startup_package_header() {
  static const uint32_t STARTUP_HEADER_LENGTH = 8u;

  return receive_bytes_async(STARTUP_HEADER_LENGTH)
    .then(boost::launch::sync, [](boost::future<InputPacket> packet) {
      return PostgresWireHandler::handle_startup_package(packet.get());
    });
}

boost::future<void> ClientConnection::receive_startup_package_contents(uint32_t size) {
  return receive_bytes_async(size)
    .then(boost::launch::sync, [=](boost::future<InputPacket> packet) {
      auto p = packet.get();
      // Read these values and ignore them
      PostgresWireHandler::handle_startup_package_content(p, size);
    });
}

boost::future<void> ClientConnection::send_ssl_denied() {
  // Don't use new_output_packet here, because this packet has special size requirements (only contains N, no size)
  auto output_packet = std::make_shared<OutputPacket>();
  PostgresWireHandler::write_value(*output_packet, NetworkMessageType::SslNo);
  
  return send_bytes_async(output_packet, true)
    .then(boost::launch::sync, [](boost::future<unsigned long> f){ f.get(); });
}

boost::future<void> ClientConnection::send_auth() {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::AuthenticationRequest);
  PostgresWireHandler::write_value(*output_packet, htonl(0u));

  return send_bytes_async(output_packet).then(boost::launch::sync, [](boost::future<unsigned long> f){ f.get(); });
}

boost::future<void> ClientConnection::send_ready_for_query() {
  // ReadyForQuery packet 'Z' with transaction status Idle 'I'
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ReadyForQuery);
  PostgresWireHandler::write_value(*output_packet, TransactionStatusIndicator::Idle);

  return send_bytes_async(output_packet, true)
    .then(boost::launch::sync, [](boost::future<unsigned long> f){ f.get(); });
}

boost::future<InputPacket> ClientConnection::receive_bytes_async(size_t size) {
  auto result = std::make_shared<InputPacket>();
  result->data.reserve(size);
  return _socket.async_read_some(boost::asio::buffer(result->data, size), boost::asio::use_boost_future)
    .then(boost::launch::sync, [=](boost::future<unsigned long> f) {
      auto received_size = f.get();  // Throws any errors
      
      Assert(received_size == size, "Client sent less data than expected.");
      
      result->offset = result->data.begin();
      return std::move(*result);
    });
}

boost::future<unsigned long> ClientConnection::send_bytes_async(std::shared_ptr<OutputPacket> packet, bool flush) {
  auto packet_size = packet->data.size();
  
  // If the packet is SslNo (size == 1), it has a special format and does not require a size
  if (packet_size > 1) {
    PostgresWireHandler::write_output_packet_size(*packet);
  }
  
  auto perform_flush = [=]() {
    return flush_async().then(boost::launch::sync, [=](boost::future<unsigned long> f) {
      auto sent_bytes = f.get();
      Assert(sent_bytes == _response_buffer.size(), "Could not send all data");
      _response_buffer.clear();
      return sent_bytes;
    });
  };

  if (_response_buffer.size() + packet->data.size() > _max_response_size) {
    // We have to flush before we can actually process the data 
    return perform_flush().then(boost::launch::sync, [=](boost::future<unsigned long> f) {
      f.get();
      return send_bytes_async(packet, flush);
    }).unwrap();
  }

  _response_buffer.insert(_response_buffer.end(), packet->data.begin(), packet->data.end());
  
  if (flush) {
    return perform_flush().then(boost::launch::sync, [=](boost::future<unsigned long> f) {
      f.get();
      return packet_size;
    });
  } else {
    // Return an already resolved future (we have just written data to the buffer)
    return boost::make_ready_future<unsigned long>(packet_size);
  }
}

boost::future<unsigned long> ClientConnection::flush_async() {
  return _socket.async_send(boost::asio::buffer(_response_buffer), boost::asio::use_boost_future);
}


} // namespace opossum