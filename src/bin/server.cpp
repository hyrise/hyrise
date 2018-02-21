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

    // Generate some data for testing
    auto table = tpcc::TpccTableGenerator::generate_tpcc_table("ITEM");
    opossum::StorageManager::get().add_table("ITEM", table);

    auto table2 = opossum::load_table("src/test/tables/int.tbl", 100);
    opossum::StorageManager::get().add_table("foo", table2);

    // Install a multi threaded scheduler
    opossum::CurrentScheduler::set(
        std::make_shared<opossum::NodeQueueScheduler>(opossum::Topology::create_numa_topology()));

    boost::asio::io_service io_service;

    opossum::Server server(io_service, port);

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
