#pragma once

#include <gmock/gmock.h>
// #include <boost/thread/future.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include "server/postgres_protocol_handler.hpp"
#include "server/ring_buffer.hpp"
#include <filesystem>
#include <fstream>
#include <stdio.h>

namespace opossum {

static const std::string file_name = "socket_file.txt";

typedef boost::asio::posix::stream_descriptor file_stream;

class MockSocket {
 public:
  MockSocket() {
    boost::asio::io_service io_service;
    auto file_descriptor = open(file_name.c_str(), O_RDWR | O_CREAT | O_APPEND, 0755);
    stream = std::make_shared<file_stream>(io_service, file_descriptor);
    io_service.run();
  }

  ~MockSocket() { std::filesystem::remove(std::filesystem::path{file_name}); }

  std::shared_ptr<file_stream> get_socket() { return stream;}

  void write(const std::string& value) {
    std::ofstream(std::filesystem::path{file_name}, std::ios_base::app) << value;
  }

  std::string read() {
    std::ifstream file_stream(file_name);
      return {std::istreambuf_iterator<char>(file_stream), std::istreambuf_iterator<char>()};
  }

  bool empty() {
    return std::ifstream(std::filesystem::path{file_name}).peek() == std::ifstream::traits_type::eof();
  }
 std::shared_ptr<file_stream> stream;
};

}  // namespace opossum
