#include "file_io_read_micro_benchmark.hpp"

namespace hyrise {

class FileIOReadMmapBenchmarkFixture : public FileIOMicroReadBenchmarkFixture {};

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping failed: ", errno));

    madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);

    auto sum = uint64_t{0};
    for (size_t index = 0; index < NUMBER_OF_ELEMENTS; ++index) {
      sum += map[random_indices[index]];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  }
  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

    madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);

    auto sum = uint64_t{0};
    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      sum += map[index];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_SHARED, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping failed: ", errno));

    madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);

    auto sum = uint64_t{0};
    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      sum += map[random_indices[index]];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_SHARED, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping failed: ", errno));

    madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);

    auto sum = uint64_t{0};
    for (size_t index = 0; index < NUMBER_OF_ELEMENTS; ++index) {
      sum += map[index];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  }

  close(fd);
}

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)->Arg(10)->Arg(100)->Arg(1000);

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise
