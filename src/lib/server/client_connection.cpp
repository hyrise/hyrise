#include "client_connection.hpp"

#include "use_boost_future.hpp"
#include "postgres_wire_handler.hpp"
#include "then_operator.hpp"

namespace opossum {

using opossum::then_operator::then;

boost::future<uint32_t> ClientConnection::receive_startup_packet_header() {
  static const uint32_t STARTUP_HEADER_LENGTH = 8u;
  
  return _receive_bytes_async(STARTUP_HEADER_LENGTH)
    >> then >> PostgresWireHandler::handle_startup_package;
}


boost::future<void> ClientConnection::receive_startup_packet_contents(uint32_t size) {
  return _receive_bytes_async(size) >> then >> [=](InputPacket p) {
    // Read these values and ignore them
    PostgresWireHandler::handle_startup_package_content(p, size);
  };
}

boost::future<RequestHeader> ClientConnection::receive_packet_header() {
  static const uint32_t HEADER_LENGTH = 5u;

  return _receive_bytes_async(HEADER_LENGTH)
    >> then >> PostgresWireHandler::handle_header;
}

boost::future<InputPacket> ClientConnection::receive_packet_contents(uint32_t size) {
  // TODO: Maybe we could offer a version of this where the returned packet is already parsed
  return _receive_bytes_async(size);
}

boost::future<void> ClientConnection::send_ssl_denied() {
  // Don't use new_output_packet here, because this packet has special size requirements (only contains N, no size)
  auto output_packet = std::make_shared<OutputPacket>();
  PostgresWireHandler::write_value(*output_packet, NetworkMessageType::SslNo);
  
  return _send_bytes_async(output_packet, true)
    >> then >> [](unsigned long) { /* ignore the result */ };
}

boost::future<void> ClientConnection::send_auth() {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::AuthenticationRequest);
  PostgresWireHandler::write_value(*output_packet, htonl(0u));

  return _send_bytes_async(output_packet)
    >> then >> [](unsigned long) { /* ignore the result */ };
}

boost::future<void> ClientConnection::send_ready_for_query() {
  // ReadyForQuery packet 'Z' with transaction status Idle 'I'
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ReadyForQuery);
  PostgresWireHandler::write_value(*output_packet, TransactionStatusIndicator::Idle);

  return _send_bytes_async(output_packet, true)
    >> then >> [](unsigned long) { /* ignore the result */ };
}

boost::future<void> ClientConnection::send_error(const std::string& message) {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ErrorResponse);

  // An error response has to include at least one identified field

  // Send the error message
  PostgresWireHandler::write_value(*output_packet, 'M');
  PostgresWireHandler::write_string(*output_packet, message);

  // Terminate the error response
  PostgresWireHandler::write_value(*output_packet, '\0');
  return _send_bytes_async(output_packet, true)
    >> then >> [](unsigned long) { /* ignore the result */ };
}

boost::future<void> ClientConnection::send_notice(const std::string& notice) {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::Notice);

  PostgresWireHandler::write_value(*output_packet, 'M');
  PostgresWireHandler::write_string(*output_packet, notice);

  // Terminate the notice response
  PostgresWireHandler::write_value(*output_packet, '\0');
  return _send_bytes_async(output_packet, true)
    >> then >> [](unsigned long) { /* ignore the result */ }; 
}

boost::future<InputPacket> ClientConnection::_receive_bytes_async(size_t size) {
  auto result = std::make_shared<InputPacket>();
  result->data.resize(size);
  
  return _socket.async_read_some(
    boost::asio::buffer(result->data, size), 
    boost::asio::use_boost_future
  ) >> then >> [=](unsigned long received_size) {
    Assert(received_size == size, "Client sent less data than expected.");
    
    result->offset = result->data.begin();
    return std::move(*result);
  };
}

boost::future<unsigned long> ClientConnection::_send_bytes_async(std::shared_ptr<OutputPacket> packet, bool flush) {
  auto packet_size = packet->data.size();
  
  // If the packet is SslNo (size == 1), it has a special format and does not require a size
  if (packet_size > 1) {
    PostgresWireHandler::write_output_packet_size(*packet);
  }

  if (_response_buffer.size() + packet->data.size() > _max_response_size) {
    // We have to flush before we can actually process the data 
    return _flush_async() >> then >> [=](unsigned long) { return _send_bytes_async(packet, flush); };
  }

  _response_buffer.insert(_response_buffer.end(), packet->data.begin(), packet->data.end());
  
  if (flush) {
    return _flush_async() >> then >> [=](unsigned long) { return packet_size; };
  } else {
    // Return an already resolved future (we have just written data to the buffer)
    return boost::make_ready_future<unsigned long>(packet_size);
  }
}

boost::future<unsigned long> ClientConnection::_flush_async() {
  return _socket.async_send(boost::asio::buffer(_response_buffer), boost::asio::use_boost_future)
    >> then >> [=](unsigned long sent_bytes) {
      Assert(sent_bytes == _response_buffer.size(), "Could not send all data");
      _response_buffer.clear();
      return sent_bytes;
    };
}


} // namespace opossum