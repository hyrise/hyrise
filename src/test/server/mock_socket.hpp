#pragma once

#include <filesystem>
#include <fstream>

#include <boost/asio.hpp>

namespace opossum {

using AsioStreamDescriptor = boost::asio::posix::stream_descriptor;

// Instead of writing data to the network device this class puts the information into a file. This allows an easier
// verification of input and output and works independently from the network.
class MockSocket {
 public:
  static constexpr char filename[] = "socket_file";

  MockSocket() : _path(test_data_path + filename) {
    _file_descriptor = open(_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0755);
    _stream = std::make_shared<AsioStreamDescriptor>(_io_service, _file_descriptor);
    _io_service.run();
  }

  ~MockSocket() {
    close(_file_descriptor);
    std::filesystem::remove(std::filesystem::path{_path});
  }

  std::shared_ptr<AsioStreamDescriptor> get_socket() { return _stream; }

  void write(const std::string& value) { std::ofstream(std::filesystem::path{_path}, std::ios_base::app) << value; }

  std::string read() {
    std::ifstream AsioStreamDescriptor(_path);
    return {std::istreambuf_iterator<char>(AsioStreamDescriptor), std::istreambuf_iterator<char>()};
  }

  bool empty() { return std::ifstream(std::filesystem::path{_path}).peek() == std::ifstream::traits_type::eof(); }

 private:
  const std::string _path;
  int _file_descriptor;
  boost::asio::io_service _io_service;
  std::shared_ptr<AsioStreamDescriptor> _stream;
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
