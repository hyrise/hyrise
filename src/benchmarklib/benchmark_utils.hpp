#pragma once

#include <cxxopts.hpp>
#include <json.hpp>

#include <tbb/concurrent_vector.h>
#include <chrono>
#include <iostream>
#include <unordered_map>

#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "encoding_config.hpp"
#include "benchmark_config.hpp"

namespace opossum {


/**
 * @return std::cout if `verbose` is true, otherwise returns a discarding stream
 */
std::ostream& get_out_stream(const bool verbose);

struct QueryBenchmarkResult : public Noncopyable {
  QueryBenchmarkResult() { iteration_durations.reserve(1'000'000); }

  QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept {
    num_iterations.store(other.num_iterations);
    duration = std::move(other.duration);
    iteration_durations = std::move(other.iteration_durations);
  }

  QueryBenchmarkResult& operator=(QueryBenchmarkResult&&) = default;

  std::atomic<size_t> num_iterations = 0;
  Duration duration = Duration{};
  tbb::concurrent_vector<Duration> iteration_durations;
};

/**
 * Loosely copying the functionality of benchmark::State
 * keep_running() returns false once enough iterations or time has passed.
 */
struct BenchmarkState {
  enum class State { NotStarted, Running, Over };

  explicit BenchmarkState(const Duration max_duration);

  bool keep_running();
  void set_done();
  bool is_done();

  State state{State::NotStarted};
  TimePoint benchmark_begin = TimePoint{};
  Duration benchmark_duration = Duration{};

  Duration max_duration;
};

class BenchmarkTableEncoder {
 public:
  // @param out   stream for logging info
  static void encode(const std::string& table_name, const std::shared_ptr<Table>& table,
                     const EncodingConfig& encoding_config, std::ostream& out);
};

class CLIConfigParser {
 public:
  static bool cli_has_json_config(const int argc, char** argv);

  static nlohmann::json parse_json_config_file(const std::string& json_file_str);

  static nlohmann::json basic_cli_options_to_json(const cxxopts::ParseResult& parse_result);

  static BenchmarkConfig parse_basic_options_json_config(const nlohmann::json& json_config);

  static BenchmarkConfig parse_basic_cli_options(const cxxopts::ParseResult& parse_result);

  static EncodingConfig parse_encoding_config(const std::string& encoding_file_str);

  static std::string detailed_help(const cxxopts::Options& options);
};

}  // namespace opossum
