#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/scheduler/current_scheduler.hpp"
#include "../../lib/scheduler/node_queue_scheduler.hpp"
#include "../../lib/scheduler/topology.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"
#include "../../tpcc/table_generator.hpp"

namespace opossum {

// Defining the base fixture class
class TPCCBenchmarkFixture : public benchmark::Fixture {
 public:
  TPCCBenchmarkFixture() {
    // Generating TPCC tables
    _gen.add_all_tables(opossum::StorageManager::get());
//    CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
  }

  virtual void TearDown(const ::benchmark::State&) {
//    CurrentScheduler::set(nullptr);
  }

 protected:
  tpcc::TableGenerator _gen;

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
