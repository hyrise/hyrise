#pragma once

#include <memory>

#include "benchmark/benchmark.h"
#include "micro_benchmark_utils.hpp"
#include "types.hpp"

namespace hyrise {

class TableWrapper;

// Defining the base fixture class
class MicroBenchmarkBasicFixture : public benchmark::Fixture {
 public:
  void SetUp(::benchmark::State& /*state*/) override;
  void TearDown(::benchmark::State& /*state*/) override;

 protected:
  void _clear_cache();

  /*
   * Recives a memory size in MB, allinges it to a passed page size and translates it to bytes.
   * Per default the UMAP page size is used.
   * 
   * @param amount of memory in MB
   * @param page size
   * @return alligned amound of memory in bytes
   */
  uint32_t _align_to_pagesize(uint32_t buffer_size_mb, uint32_t page_size = 4096);

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper_a;
  std::shared_ptr<TableWrapper> _table_wrapper_b;
  std::shared_ptr<TableWrapper> _table_dict_wrapper;
};

}  // namespace hyrise
