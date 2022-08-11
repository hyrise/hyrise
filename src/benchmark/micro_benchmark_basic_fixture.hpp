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

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper_a;
  std::shared_ptr<TableWrapper> _table_wrapper_b;
  std::shared_ptr<TableWrapper> _table_dict_wrapper;
};

}  // namespace hyrise
