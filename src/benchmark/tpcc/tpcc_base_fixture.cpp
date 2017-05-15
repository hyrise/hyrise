#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../benchmark-libs/tpcc/random_generator.hpp"
#include "../../benchmark-libs/tpcc/table_generator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/scheduler/current_scheduler.hpp"
#include "../../lib/scheduler/node_queue_scheduler.hpp"
#include "../../lib/scheduler/operator_task.hpp"
#include "../../lib/scheduler/topology.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

// Defining the base fixture class
class TPCCBenchmarkFixture : public benchmark::Fixture {
 public:
  TPCCBenchmarkFixture() : _gen(tpcc::TableGenerator()), _random_gen(tpcc::RandomGenerator()) {
    // Generating TPCC tables
    _tpcc_tables = _gen.generate_all_tables();
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

  void set_transaction_context_for_operators(const std::shared_ptr<TransactionContext> t_context,
                                             const std::vector<std::shared_ptr<AbstractOperator>> operators) {
    for (auto& op : operators) {
      op->set_transaction_context(t_context);
    }
  }

  void schedule_tasks(const std::vector<std::shared_ptr<OperatorTask>> tasks) {
    for (auto& task : tasks) {
      task->schedule();
    }
  }

  void schedule_tasks_and_wait(const std::vector<std::shared_ptr<OperatorTask>> tasks) {
    schedule_tasks(tasks);
    tasks.back()->join();
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
