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
    std::cerr << "Finished table setup" << std::endl;
  }

  void set_transaction_context_for_operators(const std::shared_ptr<TransactionContext> t_context,
                                             const std::vector<std::shared_ptr<AbstractOperator>> operators) {
    for (auto& op : operators) {
      op->set_transaction_context(t_context);
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

  // untested!!
  std::shared_ptr<std::vector<AllTypeVariant>> get_from_table_at_row(std::shared_ptr<const Table> table, size_t row) {
    auto row_counter = 0;
    for (ChunkID i = 0; i < table->chunk_count(); i++) {
      auto& chunk = table->get_chunk(i);
      // TODO(anyone): check for chunksize + row_counter == row
      if (chunk.size() + row_counter < row) {
        row_counter += chunk.size();
      } else {
        auto result = std::make_shared<std::vector<AllTypeVariant>>();
        for (ChunkID i = 0; i < chunk.col_count(); i++) {
          const auto& column = chunk.get_column(i);
          result->emplace_back(column->operator[](row - row_counter));
        }
        return result;
      }
    }
    throw std::runtime_error("trying to select row that is bigger than size of table");
  }
};
}  // namespace opossum
