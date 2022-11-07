#include "micro_benchmark_basic_fixture.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>

namespace hyrise {

const int32_t MB = 1000000;

class FileIOMicroReadBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    //TODO: Make setup/teardown global per file size to improve benchmark speed
    ssize_t BUFFER_SIZE_MB = state.range(0);

    // each int32_t contains four bytes
    int32_t vector_element_count = (BUFFER_SIZE_MB * MB) / 4;
    const auto data_to_write = std::vector<int32_t>(vector_element_count, 42);

    int32_t fd;
    if ((fd = creat("file.txt", O_WRONLY)) < 1) {
      std::cout << "create error" << std::endl;
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file

    if (write(fd, std::data(data_to_write), BUFFER_SIZE_MB * MB) != BUFFER_SIZE_MB * MB) {
      std::cout << "write error" << std::endl;
    }

    close(fd);
  }

  void TearDown(::benchmark::State& /*state*/) override {
    //TODO: Error handling
    std::remove("file.txt");
  }

 protected:
};

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC)(benchmark::State& state) {// open file
  int32_t fd;
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    std::vector<int32_t> read_data;
    read_data.reserve(NUMBER_OF_BYTES / 4);

    lseek(fd, 0, SEEK_SET);
    if (read(fd, std::data(read_data), NUMBER_OF_BYTES) != NUMBER_OF_BYTES) {
      Fail("read error: " + strerror(errno));
    }
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC)(benchmark::State& state) {
  int32_t fd;
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    std::vector<int32_t> read_data;
    read_data.reserve(NUMBER_OF_BYTES / 4);

    if (pread(fd, std::data(read_data), NUMBER_OF_BYTES, 0) != NUMBER_OF_BYTES) {
      Fail("read error: " + strerror(errno));
    }
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ)(benchmark::State& state) {// open file
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  std::vector<uint64_t> contents(NUMBER_OF_BYTES / sizeof(uint64_t));
  for(auto index = size_t{0}; index<contents.size();index++){
    contents[index] = std::rand() % UINT16_MAX;
  }

  auto control_sum = uint64_t{0};
  for(auto index = size_t{0}; index<contents.size();index++){
    control_sum += contents[index];
  }

  for (auto _ : state) {
 //   auto sum = uint64_t{0};
    uint64_t summand;

/*    for (auto iterator = contents.cbegin(); iterator < contents.end(); ++iterator) {
      summand = *iterator;
      state.PauseTiming();
      sum += summand;
      state.ResumeTiming();
    }*/
    const auto last = contents.end();
    // auto sum = uint64_t{0};
    for (auto first = contents.cbegin(); first != last; ++first) {
      summand = *first;
      (void) summand;
      state.PauseTiming();
      // sum = std::move(sum) + summand;
      state.ResumeTiming();

    }
    state.PauseTiming();
   // Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }
}

//arguments are file size in MB
// BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
// BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise