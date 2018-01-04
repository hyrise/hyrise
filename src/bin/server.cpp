#include <boost/asio/io_service.hpp>

#include <cstdlib>
#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "server/hyrise_server.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"

int main(int argc, char* argv[]) {
  try {
    auto port = 5432;

    if (argc >= 2) {
      port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    // Generate some data for testing
    auto table = tpcc::TpccTableGenerator::generate_tpcc_table("ITEM");
    opossum::StorageManager::get().add_table("ITEM", table);

    // Install a multi threaded scheduler for testing
    opossum::CurrentScheduler::set(
        std::make_shared<opossum::NodeQueueScheduler>(opossum::Topology::create_fake_numa_topology(8, 4)));

    boost::asio::io_service io_service;

    opossum::HyriseServer server(io_service, port);

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
