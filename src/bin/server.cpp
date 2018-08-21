#include <boost/asio/io_service.hpp>

#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "constant_mappings.hpp"
#include "utils/cli_options.hpp"
#include "logging/logger.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "server/server.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

int main(int argc, char* argv[]) {
  auto cli_options = opossum::CLIOptions::get_basic_cli_options("server");

  // clang-format off
  cli_options.add_options()
      ("port", "Specify the port", cxxopts::value<uint16_t>()->default_value("5432")); // NOLINT
  // clang-format on

  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (cli_parse_result.count("help")) {
    std::cout << cli_options.help() << std::endl;
    return 0;
  }

  try {
    auto port = cli_parse_result["port"].as<uint16_t>();
    Assert(port != 0 && port <= 65535, "invalid port number");
    auto logger_implementation = opossum::logger_to_string.right.at(cli_parse_result["logger"].as<std::string>());
    auto logger_format = opossum::log_format_to_string.right.at(cli_parse_result["log_format"].as<std::string>());
    auto data_path = cli_parse_result["data_path"].as<std::string>();

    // Set scheduler so that the server can execute the tasks on separate threads.
    opossum::CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());

    boost::asio::io_service io_service;

    // The server registers itself to the boost io_service. The io_service is the main IO control unit here and it lives
    // until the server doesn't request any IO any more, i.e. is has terminated. The server requests IO in its
    // constructor and then runs forever.
    opossum::Server server{io_service, port, data_path, logger_implementation, logger_format};

    std::cout << "Port: " << port << std::endl;
    std::cout << "PID: " << ::getpid() << std::endl;

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
