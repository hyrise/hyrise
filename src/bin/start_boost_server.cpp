// #include <boost/asio/io_service.hpp>
#include "boost_server/server.hpp"
#include "cxxopts.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"

cxxopts::Options get_server_cli_options() {
  cxxopts::Options cli_options("./hyriseServer", "Starts Hyrise Server in order to accept network requests.");

  // clang-format off
  cli_options.add_options()
    ("help", "Display this help and exit")
    ("p,port", "Specify the port number. 0 means randomly select an available one", cxxopts::value<uint16_t>()->default_value("5432"));  //NOLINT

  // clang-format on

  return cli_options;
}

int main(int argc, char* argv[]) {
  auto cli_options = get_server_cli_options();
  const auto parsed_options = cli_options.parse(argc, argv);

  if (parsed_options.count("help")) {
    std::cout << cli_options.help() << std::endl;
    return 0;
  }

  // Set scheduler so that the server can execute the tasks on separate threads.
  opossum::CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());

  auto port = parsed_options["port"].as<uint16_t>();
  opossum::Server server{port};
  server.run();

  return 0;
}
