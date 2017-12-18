#include <cstdlib>
#include <iostream>

#include <boost/asio/io_service.hpp>

#include <server/hyrise_server.hpp>

int main(int argc, char* argv[]) {
  try {
    auto port = 5432;

    if (argc >= 2) {
      port = std::atoi(argv[1]);
    }

    boost::asio::io_service io_service;

    opossum::HyriseServer server(io_service, port);

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
