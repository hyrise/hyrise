#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../lib/operators/table_wrapper.hpp"
#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/types.hpp"
#include "table_generator.hpp"

namespace opossum {

// Defining the base fixture class
class BenchmarkFixture : public benchmark::Fixture {
 public:
  BenchmarkFixture() {
    // Generating a test table with generate_table function from table_generator.cpp

    auto table_generator = std::make_shared<TableGenerator>();

    auto table = table_generator->get_table();

    auto table_generator2 = std::make_shared<TableGenerator>();

    auto table2 = table_generator2->get_table();

    _table_wrapper_a = std::make_shared<TableWrapper>(table_generator->get_table());
    _table_wrapper_b = std::make_shared<TableWrapper>(table_generator2->get_table());
    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
  }

  virtual void TearDown(const ::benchmark::State&) { opossum::StorageManager::get().reset(); }

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper_a;
  std::shared_ptr<TableWrapper> _table_wrapper_b;

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
