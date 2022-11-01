#include "micro_benchmark_basic_fixture.hpp"

#include <unistd.h>
#include <iostream>

namespace hyrise {

// Defining the base fixture class
    class FileIOMicroReadBenchmarkFixture : public MicroBenchmarkBasicFixture {
    public:
        void SetUp(::benchmark::State& /*state*/) override {
          std::cout << "hello" << std::endl;
        }

        // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
        void TearDown(::benchmark::State& /*state*/) override {}

    };

    BENCHMARK_F(FileIOMicroReadBenchmarkFixture, BM_TPCHQ6FirstScanPredicate)(benchmark::State& state) {
        for (auto _ : state) {
            usleep(100000);
        }
    }

}// namespace hyrise