#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../benchmark-libs/tpcc/tpcc_random_generator.hpp"
#include "../../benchmark-libs/tpcc/tpcc_table_generator.hpp"
#include "operators/get_table.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

// Defining the base fixture class
class TPCCBenchmarkFixture : public benchmark::Fixture {
 public:
  TPCCBenchmarkFixture() : _gen(tpcc::TableGenerator()), _random_gen(tpcc::RandomGenerator()) {
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

  void TearDown(::benchmark::State&) override {
    opossum::StorageManager::get().reset();
    // CurrentScheduler::set(nullptr);
  }

  void SetUp(::benchmark::State&) override {
    for (auto it = _tpcc_tables->begin(); it != _tpcc_tables->end(); ++it) {
      opossum::StorageManager::get().add_table(it->first, it->second);
    }
  }

 protected:
  tpcc::TableGenerator _gen;
  tpcc::RandomGenerator _random_gen;
  std::shared_ptr<std::map<std::string, std::shared_ptr<Table>>> _tpcc_tables;

  void clear_cache() {
    std::vector<int> clear = std::vector<int>();
    clear.resize(500 * 1000 * 1000, 42);
    for (uint i = 0; i < clear.size(); i++) {
      clear[i] += 1;
    }
    clear.resize(0);
  }
};
}  // namespace opossum
