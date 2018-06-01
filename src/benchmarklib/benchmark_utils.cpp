#include "benchmark_utils.hpp"

namespace opossum {

std::ostream& get_out_stream(const bool verbose) {
  if (verbose) {
    return std::cout;
  }

  // Create no-op stream that just swallows everything streamed into it
  // See https://stackoverflow.com/a/11826666
  class NullBuffer : public std::streambuf {
   public:
    int overflow(int c) { return c; }
  };

  static NullBuffer null_buffer;
  static std::ostream null_stream(&null_buffer);
  return null_stream;
}

BenchmarkState::BenchmarkState(const size_t max_num_iterations, const opossum::Duration max_duration)
    : max_num_iterations(max_num_iterations), max_duration(max_duration) {}

bool BenchmarkState::keep_running() {
  switch (state) {
    case State::NotStarted:
      begin = std::chrono::high_resolution_clock::now();
      state = State::Running;
      break;
    case State::Over:
      return false;
    default: {}
  }

  if (num_iterations >= max_num_iterations) {
    end = std::chrono::high_resolution_clock::now();
    state = State::Over;
    return false;
  }

  end = std::chrono::high_resolution_clock::now();
  const auto duration = end - begin;
  if (duration >= max_duration) {
    state = State::Over;
    return false;
  }

  num_iterations++;

  return true;
}

BenchmarkConfig::BenchmarkConfig(const BenchmarkMode benchmark_mode, const bool verbose, const ChunkOffset chunk_size,
                                 const EncodingType encoding_type, const size_t max_num_query_runs,
                                 const Duration& max_duration, const UseMvcc use_mvcc,
                                 const std::optional<std::string>& output_file_path, const bool enable_scheduler,
                                 const bool enable_visualization, std::ostream& out)
    : benchmark_mode(benchmark_mode),
      verbose(verbose),
      chunk_size(chunk_size),
      encoding_type(encoding_type),
      max_num_query_runs(max_num_query_runs),
      max_duration(max_duration),
      use_mvcc(use_mvcc),
      output_file_path(output_file_path),
      enable_scheduler(enable_scheduler),
      enable_visualization(enable_visualization),
      out(out) {}

}  // namespace opossum
