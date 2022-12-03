#include "file_io_write_micro_benchmark.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <numeric>

namespace hyrise {

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)(benchmark::State& state) {
  auto fd = int32_t{};
  if ((fd = open(filename, O_WRONLY)) < 0) {
    close(fd);
    Fail("Open error:" + std::strerror(errno));
  }

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);
    if (write(fd, std::data(data_to_write), NUMBER_OF_BYTES) != NUMBER_OF_BYTES) {
      close(fd);
      Fail("Write error:" + std::strerror(errno));
    }

    state.PauseTiming();
    sanity_check();
    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)(benchmark::State& state) {
  auto fd = int32_t{};
  if ((fd = open(filename, O_WRONLY)) < 0) {
    close(fd);
    Fail("Open error:" + std::strerror(errno));
  }

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    if (pwrite(fd, std::data(data_to_write), NUMBER_OF_BYTES, 0) != NUMBER_OF_BYTES) {
      close(fd);
      Fail("Write error:" + std::strerror(errno));
    }

    state.PauseTiming();
    sanity_check();
    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)(benchmark::State& state) {
  std::vector<uint32_t> copy_of_contents;

  for (auto _ : state) {
    copy_of_contents = data_to_write;
    state.PauseTiming();
    Assert(std::equal(copy_of_contents.begin(), copy_of_contents.end(), data_to_write.begin()),
           "Sanity check failed: Not the same result");
    Assert(&copy_of_contents != &data_to_write, "Sanity check failed: Same reference");
    state.ResumeTiming();
  }
}

// Arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise
