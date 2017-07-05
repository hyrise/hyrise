#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../benchmark-libs/tpcc/random_generator.hpp"
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
    // We currently run the benchmarks without a scheduler because there seem to be some problems when it is activated.
    // TODO(mp): investigate and fix.
    //    CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
  }

  virtual void TearDown(const ::benchmark::State&) {
    opossum::StorageManager::get().reset();
    // CurrentScheduler::set(nullptr);
  }

  virtual void SetUp(const ::benchmark::State&) {
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
