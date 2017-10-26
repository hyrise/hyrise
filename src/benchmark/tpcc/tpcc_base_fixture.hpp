#pragma once

#include <map>
#include <memory>
#include <string>

#include "benchmark/benchmark.h"

#include "tpcc/tpcc_random_generator.hpp"
#include "tpcc/tpcc_table_generator.hpp"

namespace opossum {

class Table;

// Defining the base fixture class
class TPCCBenchmarkFixture : public benchmark::Fixture {
 public:
  TPCCBenchmarkFixture();

  void TearDown(::benchmark::State&) override;
  void SetUp(::benchmark::State&) override;

 protected:
  tpcc::TpccTableGenerator _gen;
  tpcc::TpccRandomGenerator _random_gen;
  std::map<std::string, std::shared_ptr<Table>> _tpcc_tables;

  void clear_cache();
};

}  // namespace opossum
