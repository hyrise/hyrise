#include <boost/asio/io_service.hpp>

#include <cstdlib>
#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "server/server.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"
#include "concurrency/logging/logger.hpp"

int main(int argc, char* argv[]) {
  try {
    uint16_t port = 5432;
    if (argc >= 2) {
      port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    opossum::Logger::Implementation implementation = opossum::Logger::default_implementation;
    if (argc >= 3) {
      if (argv[2] == std::string("SimpleLogger")) {
        implementation = opossum::Logger::Implementation::Simple;
      } else if (argv[2] == std::string("GroupCommitLogger")) {
        implementation = opossum::Logger::Implementation::GroupCommit;
      } else {
        std::cout << "Unkown logger: " << argv[2] << std::endl;
        return 1;
      }
    }

    std::string logging_path = opossum::Logger::default_data_path;
    if (argc >= 4) {
      logging_path = argv[3];
    }

    // Set scheduler so that the server can execute the tasks on separate threads.
    opossum::CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());

    boost::asio::io_service io_service;

    // The server registers itself to the boost io_service. The io_service is the main IO control unit here and it lives
    // until the server doesn't request any IO any more, i.e. is has terminated. The server requests IO in its
    // constructor and then runs forever.
    opossum::Server server{io_service, port, logging_path, implementation};

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
