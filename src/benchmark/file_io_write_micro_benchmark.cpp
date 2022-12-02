#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <numeric>
#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

const auto MB = uint32_t{1'000'000};

class FileIOWriteMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    // TODO(phoeinx): Make setup/teardown global per file size to improve benchmark speed
    ssize_t BUFFER_SIZE_MB = state.range(0);
    // each uint32_t contains four bytes
    vector_element_count = (BUFFER_SIZE_MB * MB) / sizeof(uint32_t);
    data_to_write = std::vector<uint32_t>(vector_element_count, VALUE_TO_WRITE);
    control_sum = vector_element_count * uint64_t{VALUE_TO_WRITE};

    if (creat(filename, O_RDWR) < 1) {
      Fail("Create error:" + std::strerror(errno));
    }
    chmod(filename, S_IRUSR | S_IWUSR);  // enables owner to read and write file
  }

  void TearDown(::benchmark::State& /*state*/) override {
    Assert(std::remove(filename) == 0, "Remove error: " + std::strerror(errno));
  }

 protected:
  std::vector<uint32_t> data_to_write;
  uint64_t control_sum = uint64_t{0};
  uint32_t vector_element_count;
  uint32_t VALUE_TO_WRITE = 42;
  const char* filename = "file.txt";  //const char* needed for C-System Calls

  void mmap_write_benchmark(benchmark::State& state, const int flag, const int data_access_mode, const ssize_t file_size);
  void sanity_check(uint32_t NUMBER_OF_BYTES);
};

void FileIOWriteMicroBenchmarkFixture::sanity_check(uint32_t NUMBER_OF_BYTES) {
  auto fd = int32_t{};
  if ((fd = open(filename, O_RDONLY)) < 0) {
    close(fd);
    Fail("Open error:" + std::strerror(errno));
  }


  const auto file_size = lseek(fd, 0, SEEK_END);
  Assert(file_size == NUMBER_OF_BYTES, "Sanity check failed: Actual size of " + std::to_string(file_size) +
      " does not match expected file size of " + std::to_string(NUMBER_OF_BYTES) + ".");

  auto read_data = std::vector<uint32_t>(NUMBER_OF_BYTES / sizeof(uint32_t));

  const off_t OFFSET = 0;
  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
  close(fd);

  Assert(map != MAP_FAILED, "Mapping for Sanity Check Failed:" + std::strerror(errno));

  memcpy(std::data(read_data), map, NUMBER_OF_BYTES);
  const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
  Assert(control_sum == sum,
         "Sanity check failed. Got: " + std::to_string(sum) + " Expected: " + std::to_string(control_sum));

  // Remove memory mapping after job is done.
  Assert(munmap(map, NUMBER_OF_BYTES) == 0, "Unmapping for Sanity Check failed: " + std::strerror(errno));
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)(benchmark::State& state) {
  auto fd = int32_t{};
  if ((fd = open(filename, O_WRONLY)) < 0) {
    close(fd);
    Fail("Open error:" + std::strerror(errno));
  }
  const auto NUMBER_OF_BYTES = static_cast<uint32_t>(state.range(0) * MB);

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
    sanity_check(NUMBER_OF_BYTES);
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
  const auto NUMBER_OF_BYTES = static_cast<uint32_t>(state.range(0) * MB);

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    if (pwrite(fd, std::data(data_to_write), NUMBER_OF_BYTES, 0) != NUMBER_OF_BYTES) {
      close(fd);
      Fail("Write error:" + std::strerror(errno));
    }

    state.PauseTiming();
    sanity_check(NUMBER_OF_BYTES);
    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_PRIVATE, 0, state.range(0));
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_SHARED, 0, state.range(0));
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_SHARED, 1, state.range(0));
}

/*
 * Performs a benchmark run with the given parameters. 
 * 
 * @arguments:
 *      state: the benchmark::State object handed to the called benchmarking function.
 *      flag: The mmap flag (e.g., MAP_PRIVATE or MAP_SHARED).
 *      data_access_mode: The way the data is written.
 *                  (-1)  No data access
 *                  (0)   Sequential
 *                  (1)   Random
 *      file_size: Size argument of benchmark.
*/
void FileIOWriteMicroBenchmarkFixture::mmap_write_benchmark(benchmark::State& state, const int flag,
                                                            const int data_access_mode, const ssize_t file_size) {
  const auto NUMBER_OF_BYTES = static_cast<uint32_t>(state.range(0) * MB);

  auto fd = int32_t{};
  if ((fd = open(filename, O_RDWR)) < 0) {
    close(fd);
    Fail("Open error:" + std::strerror(errno));
  }

  // set output file size to avoid mapping errors
  if (ftruncate(fd, NUMBER_OF_BYTES) < 0) {
    close(fd);
    Fail("Ftruncate error:" + std::strerror(errno));
  }

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};
    int32_t* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_WRITE, flag, fd, OFFSET));
    Assert(map != MAP_FAILED, "Mapping Failed:" + std::strerror(errno));

    switch (data_access_mode) {
      case 0:
        memcpy(map, std::data(data_to_write), NUMBER_OF_BYTES);
        break;
      case 1:
        state.PauseTiming();
        // Generating random indexes should not play a role in the benchmark.
        const auto ind_access_order = generate_random_indexes(vector_element_count);
        state.ResumeTiming();
        for (uint32_t idx = 0; idx < ind_access_order.size(); ++idx) {
          auto access_index = ind_access_order[idx];
          map[access_index] = VALUE_TO_WRITE;
        }
        break;
    }

    // After writing, sync changes to filesystem.
    Assert(msync(map, NUMBER_OF_BYTES, MS_SYNC) != -1, "Mapping Syncing Failed:" + std::strerror(errno));
    state.PauseTiming();

    // We need this because MAP_PRIVATE is copy-on-write and
    // thus written stuff is not visible in the original file.
    if (flag == MAP_PRIVATE) {
      std::vector<uint32_t> read_data;
      read_data.resize(NUMBER_OF_BYTES / sizeof(uint32_t));
      memcpy(std::data(read_data), map, NUMBER_OF_BYTES);
      auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
      Assert(control_sum == sum,
             "Sanity check failed. Got: " + std::to_string(sum) + "Expected: " + std::to_string(control_sum));
    } else {
      sanity_check(NUMBER_OF_BYTES);
    }

    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert(munmap(map, NUMBER_OF_BYTES) == 0, "Unmapping failed:" + std::strerror(errno));
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)(benchmark::State& state) {  // open file
  const auto NUMBER_OF_BYTES = static_cast<uint32_t>(state.range(0) * MB);

  std::vector<uint64_t> contents(NUMBER_OF_BYTES / sizeof(uint64_t));
  for (auto index = size_t{0}; index < contents.size(); index++) {
    contents[index] = std::rand() % UINT16_MAX;
  }
  std::vector<uint64_t> copy_of_contents;

  for (auto _ : state) {
    copy_of_contents = contents;
    state.PauseTiming();
    Assert(std::equal(copy_of_contents.begin(), copy_of_contents.end(), contents.begin()),
           "Sanity check failed: Not the same result");
    Assert(&copy_of_contents != &contents, "Sanity check failed: Same reference");
    state.ResumeTiming();
  }
}

// Arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise
