#include <iostream>

#include "benchmark/benchmark.h"

namespace opossum {

class TPCHFixture : public benchmark::Fixture {

};

void BM_TPCH_1(benchmark::State& state, bool mvcc, bool multithreaded) {
 std::cout << "Running TPCH1: " << mvcc << " " << multithreaded << std::endl;
}

//BENCHMARK_CAPTURE(BM_TPCH_1, true, false);

}