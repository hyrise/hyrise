#include "micro_benchmark_basic_fixture.hpp"

#include <unistd.h>

namespace hyrise {

// Defining the base fixture class
    class FileIOMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
    public:
        void SetUp(::benchmark::State& /*state*/) override {

        }

        // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
        void TearDown(::benchmark::State& /*state*/) override {}

    };

    BENCHMARK_F(FileIOMicroBenchmarkFixture, BM_TPCHQ6FirstScanPredicate)(benchmark::State& state) {
        for (auto _ : state) {
            usleep(100000);
        }
    }
}// namespace hyrise