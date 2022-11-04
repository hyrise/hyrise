#include "micro_benchmark_basic_fixture.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>


namespace hyrise {

const int GB = 1000000000;
const int MB = 1000000;

class FileIOWriteMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
		//TODO: Make setup/teardown global per file size to improve benchmark speed
    ssize_t BUFFER_SIZE = state.range(0);
		// each int32_t contains four bytes
    int32_t vector_element_count = BUFFER_SIZE / 4;
    data_to_write = std::vector<int32_t>(vector_element_count, 42);

    if (creat("file.txt", O_WRONLY) < 1) {
      std::cout << "create error" << std::endl;
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
  }

  void TearDown(::benchmark::State& /*state*/) override {
    //TODO: Error handling
    std::remove("file.txt");
  }

 protected:
  std::vector<int32_t> data_to_write;
};

void clear_cache() {
    //TODO: better documentation of which caches we are clearing
    sync();
    std::ofstream ofs("/proc/sys/vm/drop_caches");
    ofs << "3" << std::endl;
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)(benchmark::State& state) {// open file
  int32_t fd;
  if ((fd = open("file.txt", O_WRONLY)) < 0) {
		std::cout << "open error " << errno << std::endl;
  }
  for (auto _ : state) {
    state.PauseTiming();
    clear_cache();
    state.ResumeTiming();

    if (write(fd, std::data(data_to_write), state.range(0)) != state.range(0)) {
			std::cout << "write error " << errno << std::endl;
		}
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)(benchmark::State& state) {
  int32_t fd;
	if ((fd = open("file.txt", O_WRONLY)) < 0) {
		std::cout << "open error " << errno << std::endl;
	}
	for (auto _ : state) {
		state.PauseTiming();
		clear_cache();
		state.ResumeTiming();

		if (pwrite(fd, std::data(data_to_write), state.range(0), 0) != state.range(0)) {
			std::cout << "write error " << errno << std::endl;
		}
	}
}

BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)->Arg(10*MB)->Arg(100*MB)->Arg(1*GB);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)->Arg(10*MB)->Arg(100*MB)->Arg(1*GB);

}  // namespace hyrise