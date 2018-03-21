#include <boost/asio/io_service.hpp>

#include <cstdlib>
#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "server/server.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "utils/load_table.hpp"

int main(int argc, char* argv[]) {
  try {
    uint16_t port = 5432;

    if (argc >= 2) {
      port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    // Set scheduler so that the server can execute the tasks on separate threads.
    opossum::CurrentScheduler::set(
        std::make_shared<opossum::NodeQueueScheduler>(opossum::Topology::create_numa_topology()));

    boost::asio::io_service io_service;

    // The server registers itself to the boost io_service. The io_service is the main IO control unit here and it lives
    // until the server doesn't request any IO any more, i.e. is has terminated. The server requests IO in its
    // constructor and then runs forever.
    opossum::Server server{io_service, port};

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
