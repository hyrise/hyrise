#pragma once

#include <boost/asio.hpp>
#include <filesystem>
#include <fstream>

namespace opossum {

static constexpr char file_name[] = "socket_file";

typedef boost::asio::posix::stream_descriptor file_stream;

// Instead of writing data to the network device this class puts the information into a file. This allows an easier
// verification of input and output and works independently from the network.
class MockSocket {
 public:
  MockSocket() {
    boost::asio::io_service io_service;
    auto file_descriptor = open(file_name, O_RDWR | O_CREAT | O_APPEND, 0755);
    _stream = std::make_shared<file_stream>(io_service, file_descriptor);
    io_service.run();
  }

  ~MockSocket() { std::filesystem::remove(std::filesystem::path{file_name}); }

  std::shared_ptr<file_stream> get_socket() { return _stream; }

  void write(const std::string& value) {
    std::ofstream(std::filesystem::path{file_name}, std::ios_base::app) << value;
  }

  std::string read() {
    std::ifstream file_stream(file_name);
    return {std::istreambuf_iterator<char>(file_stream), std::istreambuf_iterator<char>()};
  }

  bool empty() { return std::ifstream(std::filesystem::path{file_name}).peek() == std::ifstream::traits_type::eof(); }

 private:
  std::shared_ptr<file_stream> _stream;
};

// Helper class to convert integers from network byte order to host byte order.
class NetworkConversionHelper {
 public:
  static uint32_t get_message_length(std::string::const_iterator start) {
    uint32_t network_value = 0;
    std::copy_n(start, sizeof(uint32_t), reinterpret_cast<char*>(&network_value));
    return ntohl(network_value);
  }

  static uint16_t get_small_int(std::string::const_iterator start) {
    uint16_t network_value = 0;
    std::copy_n(start, sizeof(uint16_t), reinterpret_cast<char*>(&network_value));
    return ntohs(network_value);
  }
};

}  // namespace opossum
