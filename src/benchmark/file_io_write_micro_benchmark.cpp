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
  uint64_t control_sum = uint64_t{0};
  uint32_t vector_element_count;
  uint32_t VALUE_TO_WRITE = 42;

  void SetUp(::benchmark::State& state) override {
    // TODO(phoeinx): Make setup/teardown global per file size to improve benchmark speed
    ssize_t BUFFER_SIZE_MB = state.range(0);
    // each uint32_t contains four bytes
    vector_element_count = (BUFFER_SIZE_MB * MB) / sizeof(uint32_t);
    data_to_write = std::vector<uint32_t>(vector_element_count, VALUE_TO_WRITE);
    control_sum = vector_element_count * uint64_t{VALUE_TO_WRITE};

    if (creat("file.txt", O_WRONLY) < 1) {
      std::cout << "create error" << std::endl;
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
  }

  void sanity_check(uint32_t NUMBER_OF_BYTES ) {
    int32_t fd;
    if ((fd = open("file.txt", O_RDONLY)) < 0) {
      std::cout << "open error " << std::strerror(errno) << std::endl;
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
    std::vector<uint32_t> read_data;
    read_data.resize(NUMBER_OF_BYTES / sizeof(uint32_t));

    // Getting the mapping to memory.
    off_t OFFSET = 0;

    uint32_t* map = reinterpret_cast<uint32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
    if (map == MAP_FAILED) {
      std::cout << "Mapping for Sanity Check Failed." << std::strerror(errno) << std::endl;
    }

    memcpy(std::data(read_data), map, NUMBER_OF_BYTES);
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed. Got: " + std::to_string(sum) + "Expected: " + std::to_string(control_sum));
    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping for Sanity Check failed." << std::endl;
    }
  }

  void TearDown(::benchmark::State& /*state*/) override {
    // TODO(phoeinx): Error handling
    std::remove("file.txt");
  }

 protected:
  std::vector<uint32_t> data_to_write;
  void mmap_write_benchmark(benchmark::State& state, const int flag, int data_access_mode, const int32_t file_size);
};

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)(benchmark::State& state) {  // open file
  int32_t fd;
  if ((fd = open("file.txt", O_WRONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    if (write(fd, std::data(data_to_write), NUMBER_OF_BYTES) != NUMBER_OF_BYTES) {
      std::cout << "write error " << errno << std::endl;
    }

    state.PauseTiming();
    sanity_check(NUMBER_OF_BYTES);
    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)(benchmark::State& state) {
  int32_t fd;
  if ((fd = open("file.txt", O_WRONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    if (pwrite(fd, std::data(data_to_write), NUMBER_OF_BYTES, 0) != NUMBER_OF_BYTES) {
      std::cout << "write error " << errno << std::endl;
    }

    state.PauseTiming();
    sanity_check(NUMBER_OF_BYTES);
    state.ResumeTiming();
  }
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
                                                            int data_access_mode, const int32_t file_size) {
  const auto NUMBER_OF_BYTES = uint32_t{static_cast<uint32_t>(state.range(0) * MB)};

  int32_t fd;
  if ((fd = open("file.txt", O_RDWR)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

  // set output file size
  if (ftruncate(fd, NUMBER_OF_BYTES) < 0) {
    std::cout << "ftruncate error " << errno << std::endl;
  }

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};
    /*
    mmap man page: 
    MAP_SHARED:
      "Updates to the mapping are visible to other processes mapping 
      the same region"
      "changes are carried through to the underlying files"
    */
    int32_t* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_WRITE, flag, fd, OFFSET));
    if (map == MAP_FAILED) {
      std::cout << "Mapping Failed. " << std::strerror(errno) << std::endl;
      continue;
    }

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
    if (msync(map, NUMBER_OF_BYTES, MS_SYNC) == -1) {
      std::cout << "Write error " << errno << std::endl;
    }

    state.PauseTiming();

    // We need this because MAP_PRIVATE is copy-on-write and
    // thus written stuff is not visible in the original file.
    if (flag == MAP_PRIVATE) {
      std::vector<uint32_t> read_data;
      read_data.resize(NUMBER_OF_BYTES / sizeof(uint32_t));
      memcpy(std::data(read_data), map, NUMBER_OF_BYTES);
      auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
      Assert(control_sum == sum, "Sanity check failed. Got: " + std::to_string(sum) + "Expected: " + std::to_string(control_sum));
    } else {
      sanity_check(NUMBER_OF_BYTES);
    }

    state.ResumeTiming();

    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping failed." << std::endl;
    }
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)(benchmark::State& state) {  // open file
  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

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
