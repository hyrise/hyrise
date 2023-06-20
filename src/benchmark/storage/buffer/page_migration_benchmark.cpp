#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

class PageMigrationFixture : public benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) {
    _mapped_region = create_mapped_region();
    volatile_regions =
        std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions(_mapped_region);
  }

  void TearDown(const ::benchmark::State& state) {
    unmap_region(_mapped_region);
  }

 protected:
  std::byte* _mapped_region;
};

void unmap_region(std::byte* region);

BENCHMARK_DEFINE_F(PageMigrationFixture, BM_MigrateFromDRAMToCXL)(benchmark::State& state) {
  auto region = volatile_regions[state.range(0)];

  for (auto _ : st) {}
}

BENCHMARK_DEFINE_F(PageMigrationFixture, BM_MigrateFromCXLToDRAM)(benchmark::State& state) {
  for (auto _ : st) {}
}

BENCHMARK_REGISTER_F(PageMigrationFixture, BM_MigrateFromDRAMToCXL)
    ->ArgsProduct({benchmark::CreateRange(64, 2048, /*multi=*/2),
                   benchmark::CreateDenseRange(MIN_PAGE_SIZE_TYPE, MAX_PAGE_SIZE_TYPE, /*step=*/1)})

}  // namespace hyrise