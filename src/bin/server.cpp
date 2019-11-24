#include "cxxopts.hpp"

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "server/server.hpp"

cxxopts::Options get_server_cli_options() {
  cxxopts::Options cli_options("./hyriseServer", "Starts Hyrise server in order to accept network requests.");

  // clang-format off
  cli_options.add_options()
    ("help", "Display this help and exit") // NOLINT
    ("address", "Specify the address to run on", cxxopts::value<std::string>()->default_value("0.0.0.0"))  // NOLINT
    ("p,port", "Specify the port number. 0 means randomly select an available one. If no port is specified, the the server will start on PostgreSQL's official port", cxxopts::value<uint16_t>()->default_value("5432"))  // NOLINT
    ("execution_info", "Send execution information after statement execution", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ;  // NOLINT
  // clang-format on

  return cli_options;
}

int main(int argc, char* argv[]) {
  auto cli_options = get_server_cli_options();
  const auto parsed_options = cli_options.parse(argc, argv);

  // Print help and exit
  if (parsed_options.count("help")) {
    std::cout << cli_options.help() << std::endl;
    return 0;
  }

  const auto execution_info = parsed_options["execution_info"].as<bool>();
  const auto port = parsed_options["port"].as<uint16_t>();

  boost::system::error_code error;
  const auto address = boost::asio::ip::make_address(parsed_options["address"].as<std::string>(), error);

  Assert(!error, "Not a valid IPv4 address: " + parsed_options["address"].as<std::string>() + ", terminating...");

  // Set scheduler so that the server can execute the tasks on separate threads.
  opossum::Hyrise::get().set_scheduler(std::make_shared<opossum::NodeQueueScheduler>());

  auto server = opossum::Server{address, port, static_cast<opossum::SendExecutionInfo>(execution_info)};
  server.run();

  return 0;
}
