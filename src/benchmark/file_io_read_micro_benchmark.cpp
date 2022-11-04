#include "micro_benchmark_basic_fixture.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>

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

void clear_cache() {
    //TODO: better documentation of which caches we are clearing
    sync();
    std::ofstream ofs("/proc/sys/vm/drop_caches");
    ofs << "3" << std::endl;
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, WRITE_NON_ATOMIC)(benchmark::State& state) {// open file
  int32_t fd;
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    clear_cache();
    state.ResumeTiming();

    std::vector<int32_t> read_data;
    read_data.reserve(NUMBER_OF_BYTES / 4);

    if (read(fd, std::data(read_data), NUMBER_OF_BYTES) != NUMBER_OF_BYTES) {
      Fail("read error: " + strerror(errno));
    }
    std::cout << "read successful" << std::endl;
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PWRITE_ATOMIC)(benchmark::State& state) {
  int32_t fd;
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    clear_cache();
    state.ResumeTiming();

    std::vector<int32_t> read_data;
    read_data.reserve(NUMBER_OF_BYTES / 4);

    if (pread(fd, std::data(read_data), NUMBER_OF_BYTES, 0) != NUMBER_OF_BYTES) {
      Fail("read error: " + strerror(errno));
    }
  }
}

//arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, WRITE_NON_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PWRITE_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise