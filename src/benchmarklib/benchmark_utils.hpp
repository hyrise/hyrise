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

namespace opossum {

/**
 * IndividualQueries runs each query a number of times and then the next one
 * PermutedQuerySet runs the queries as set permuting their order after each run (this exercises caches)
 */
enum class BenchmarkMode { IndividualQueries, PermutedQuerySet };

using Duration = std::chrono::high_resolution_clock::duration;
using TimePoint = std::chrono::high_resolution_clock::time_point;

using DataTypeEncodingMapping = std::unordered_map<DataType, SegmentEncodingSpec>;

// Map<TABLE_NAME, Map<column_name, SegmentEncoding>>
using TableSegmentEncodingMapping =
    std::unordered_map<std::string, std::unordered_map<std::string, SegmentEncodingSpec>>;

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

// View EncodingConfig::description to see format of encoding JSON
struct EncodingConfig {
  EncodingConfig();
  EncodingConfig(const SegmentEncodingSpec& default_encoding_spec, DataTypeEncodingMapping type_encoding_mapping,
                 TableSegmentEncodingMapping encoding_mapping);
  explicit EncodingConfig(const SegmentEncodingSpec& default_encoding_spec);

  static EncodingConfig unencoded();

  const SegmentEncodingSpec default_encoding_spec;
  const DataTypeEncodingMapping type_encoding_mapping;
  const TableSegmentEncodingMapping custom_encoding_mapping;

  static SegmentEncodingSpec encoding_spec_from_strings(const std::string& encoding_str,
                                                        const std::string& compression_str);
  static EncodingType encoding_string_to_type(const std::string& encoding_str);
  static std::optional<VectorCompressionType> compression_string_to_type(const std::string& compression_str);

  nlohmann::json to_json() const;

  static const char* description;
};

class BenchmarkTableEncoder {
 public:
  static void encode(const std::string& table_name, const std::shared_ptr<Table>& table, const EncodingConfig& config);
};

// View BenchmarkConfig::description to see format of the JSON-version
struct BenchmarkConfig {
  BenchmarkConfig(const BenchmarkMode benchmark_mode, const bool verbose, const ChunkOffset chunk_size,
                  const EncodingConfig& encoding_config, const size_t max_num_query_runs, const Duration& max_duration,
                  const Duration& warmup_duration, const UseMvcc use_mvcc,
                  const std::optional<std::string>& output_file_path, const bool enable_scheduler, const uint cores,
                  const uint clients, const bool enable_visualization, std::ostream& out);

  static BenchmarkConfig get_default_config();

  const BenchmarkMode benchmark_mode = BenchmarkMode::IndividualQueries;
  const bool verbose = false;
  const ChunkOffset chunk_size = 100'000;
  const EncodingConfig encoding_config = EncodingConfig{};
  const size_t max_num_query_runs = 1000;
  const Duration max_duration = std::chrono::seconds(60);
  const Duration warmup_duration = std::chrono::seconds(0);
  const UseMvcc use_mvcc = UseMvcc::No;
  const std::optional<std::string> output_file_path = std::nullopt;
  const bool enable_scheduler = false;
  const uint cores = 0;
  const uint clients = 1;
  const bool enable_visualization = false;
  std::ostream& out;

  static const char* description;

 private:
  BenchmarkConfig() : out(std::cout) {}
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
