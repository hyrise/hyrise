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
    // Remove file if it still exists, e. g. after a broken test.
    std::filesystem::remove(std::filesystem::path{file_name});
    boost::asio::io_service io_service;
    auto file_descriptor = open(file_name, O_RDWR | O_CREAT | O_APPEND, 0755);
    stream = std::make_shared<file_stream>(io_service, file_descriptor);
    io_service.run();
  }

  ~MockSocket() { std::filesystem::remove(std::filesystem::path{file_name}); }

  std::shared_ptr<file_stream> get_socket() { return stream; }

  void write(const std::string& value) { std::ofstream(std::filesystem::path{file_name}, std::ios_base::app) << value; }

  std::string read() {
    std::ifstream file_stream(file_name);
    return {std::istreambuf_iterator<char>(file_stream), std::istreambuf_iterator<char>()};
  }

  bool empty() { return std::ifstream(std::filesystem::path{file_name}).peek() == std::ifstream::traits_type::eof(); }

 private:
  std::shared_ptr<file_stream> stream;
};

}  // namespace opossum
