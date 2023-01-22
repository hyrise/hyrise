#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <filesystem>
#include <numeric>
#include <span>

#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

class FileIOMicroReadBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    const auto size_parameter = state.range(0);
    NUMBER_OF_BYTES = _align_to_pagesize(size_parameter);
    NUMBER_OF_ELEMENTS = NUMBER_OF_BYTES / uint32_t_size;
    filename = "benchmark_data_" + std::to_string(size_parameter) + ".txt";

    auto fd = int32_t{};
    if (!std::filesystem::exists(filename)) {
      numbers = generate_random_positive_numbers(NUMBER_OF_ELEMENTS);
      control_sum = std::accumulate(numbers.begin(), numbers.end(), uint64_t{0});
      Assert(((fd = creat(filename.c_str(), O_WRONLY)) >= 1),
             close_file_and_return_error_message(fd, "Create error: ", errno));
      chmod(filename.c_str(), S_IRWXU);  // enables owner to rwx file
      Assert((write(fd, std::data(numbers), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
             close_file_and_return_error_message(fd, "Write error: ", errno));
    } else {
      Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0),
             close_file_and_return_error_message(fd, "Open error: ", errno));
      const auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, 0));
      Assert((map != MAP_FAILED), close_file_and_return_error_message(fd, "Mapping Failed: ", errno));
      const auto map_span_view = std::span{map, NUMBER_OF_ELEMENTS};
      control_sum = std::accumulate(map_span_view.begin(), map_span_view.end(), uint64_t{0});
    }
    close(fd);
  }

 protected:
  const ssize_t uint32_t_size = ssize_t{sizeof(uint32_t)};
  std::string filename;
  uint64_t control_sum = uint64_t{0};
  uint32_t NUMBER_OF_BYTES = uint32_t{0};
  uint32_t NUMBER_OF_ELEMENTS = uint32_t{0};
  std::vector<uint32_t> numbers = std::vector<uint32_t>{};
  void read_non_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void read_non_atomic_single_threaded(benchmark::State& state);
  void read_non_atomic_random_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void read_non_atomic_random_single_threaded(benchmark::State& state);
  void pread_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void pread_atomic_single_threaded(benchmark::State& state);
  void pread_atomic_random_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void pread_atomic_random_single_threaded(benchmark::State& state);
#ifdef __linux__
  void libaio_sequential_read_single_threaded(benchmark::State& state);
  void libaio_sequential_read_multi_threaded(benchmark::State& state, uint16_t aio_request_count);
  void libaio_random_read(benchmark::State& state, uint16_t aio_request_count);
#endif
  void memory_mapped_read_single_threaded(benchmark::State& state, const int mapping_type, const int map_mode_flag, const int access_order);
  #ifdef __linux__
  void memory_mapped_read_user_space(benchmark::State& state, const uint16_t thread_count, const int access_order);
  #endif
  void memory_mapped_read_multi_threaded(benchmark::State& state, const int mapping_type, const int map_mode_flag, const uint16_t thread_count, const int access_order);


  // enums for mmap benchmarks
  enum MAPPING_TYPE { MMAP, UMAP };

  enum DATA_ACCESS_TYPES { SEQUENTIAL, RANDOM };

  enum MAP_ACCESS_TYPES { SHARED = MAP_SHARED, PRIVATE = MAP_PRIVATE };
};
}  // namespace hyrise
