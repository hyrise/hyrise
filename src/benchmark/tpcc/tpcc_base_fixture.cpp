#include "tpcc_base_fixture.hpp"

#include <iostream>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

TPCCBenchmarkFixture::TPCCBenchmarkFixture()
    : _gen(tpcc::TpccTableGenerator()), _random_gen(tpcc::TpccRandomGenerator()) {
  // TODO(mp): This constructor is currently run once before each TPCC benchmark.
  // Thus we create all tables up to 8 times, which takes quite a long time.
  std::cout << "Generating tables (this might take a couple of minutes)..." << std::endl;
  // Generating TPCC tables
  _tpcc_tables = _gen.generate_all_tables();
  // We currently run the benchmarks without a scheduler because there are problems when it is activated.
  // The Sort in TPCCDeliveryBenchmark-BM_delivery crashes because of a access @0 in a vector of length 0
  // TODO(mp): investigate and fix.
  // CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(4, 2)));
}

void TPCCBenchmarkFixture::TearDown(::benchmark::State&) {
  StorageManager::get().reset();
  // CurrentScheduler::set(nullptr);
}

void TPCCBenchmarkFixture::SetUp(::benchmark::State&) {
  for (auto it = _tpcc_tables.begin(); it != _tpcc_tables.end(); ++it) {
    StorageManager::get().add_table(it->first, it->second);
  }
}

void TPCCBenchmarkFixture::clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

}  // namespace opossum
