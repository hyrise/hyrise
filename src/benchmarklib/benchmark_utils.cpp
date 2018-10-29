#include <boost/algorithm/string.hpp>

#include <fstream>

#include "benchmark_utils.hpp"
#include "constant_mappings.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "storage/table.hpp"
#include "utils/filesystem.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

std::ostream& get_out_stream(const bool verbose) {
  if (verbose) {
    return std::cout;
  }

  // Create no-op stream that just swallows everything streamed into it
  // See https://stackoverflow.com/a/11826666
  class NullBuffer : public std::streambuf {
   public:
    int overflow(int c) override { return c; }
  };

  static NullBuffer null_buffer;
  static std::ostream null_stream(&null_buffer);
  return null_stream;
}

BenchmarkState::BenchmarkState(const opossum::Duration max_duration) : max_duration(max_duration) {}

bool BenchmarkState::keep_running() {
  switch (state) {
    case State::NotStarted:
      benchmark_begin = std::chrono::high_resolution_clock::now();
      state = State::Running;
      break;
    case State::Over:
      return false;
    default: {}
  }

  benchmark_duration = std::chrono::high_resolution_clock::now() - benchmark_begin;

  // Stop execution if we reached the time limit
  if (benchmark_duration >= max_duration) {
    set_done();
    return false;
  }

  return true;
}

void BenchmarkState::set_done() { state = State::Over; }

bool BenchmarkState::is_done() { return state == State::Over; }

BenchmarkConfig::BenchmarkConfig(const BenchmarkMode benchmark_mode, const bool verbose, const ChunkOffset chunk_size,
                                 const EncodingConfig& encoding_config, const size_t max_num_query_runs,
                                 const Duration& max_duration, const Duration& warmup_duration, const UseMvcc use_mvcc,
                                 const std::optional<std::string>& output_file_path, const bool enable_scheduler,
                                 const uint cores, const uint clients, const bool enable_visualization,
                                 std::ostream& out)
    : benchmark_mode(benchmark_mode),
      verbose(verbose),
      chunk_size(chunk_size),
      encoding_config(encoding_config),
      max_num_query_runs(max_num_query_runs),
      max_duration(max_duration),
      warmup_duration(warmup_duration),
      use_mvcc(use_mvcc),
      output_file_path(output_file_path),
      enable_scheduler(enable_scheduler),
      cores(cores),
      clients(clients),
      enable_visualization(enable_visualization),
      out(out) {}

BenchmarkConfig BenchmarkConfig::get_default_config() { return BenchmarkConfig(); }

bool CLIConfigParser::cli_has_json_config(const int argc, char** argv) {
  const auto has_json = argc > 1 && boost::algorithm::ends_with(argv[1], ".json");
  if (has_json && argc > 2) {
    std::cout << "Passed multiple args with a json config. All CLI args will be ignored...";
  }

  return has_json;
}

nlohmann::json CLIConfigParser::parse_json_config_file(const std::string& json_file_str) {
  Assert(filesystem::is_regular_file(json_file_str), "No such file: " + json_file_str);

  nlohmann::json json_config;
  std::ifstream json_file{json_file_str};
  json_file >> json_config;

  return json_config;
}

BenchmarkConfig CLIConfigParser::parse_basic_options_json_config(const nlohmann::json& json_config) {
  const auto default_config = BenchmarkConfig::get_default_config();

  // Should the benchmark be run in verbose mode
  const auto verbose = json_config.value("verbose", default_config.verbose);
  auto& out = get_out_stream(verbose);

  // Display info about output destination
  std::optional<std::string> output_file_path;
  const auto output_file_string = json_config.value("output", "");
  if (!output_file_string.empty()) {
    output_file_path = output_file_string;
    out << "- Writing benchmark results to '" << *output_file_path << "'" << std::endl;
  } else {
    out << "- Writing benchmark results to stdout" << std::endl;
  }

  // Display info about MVCC being enabled or not
  const auto enable_mvcc = json_config.value("mvcc", default_config.use_mvcc == UseMvcc::Yes);
  const auto use_mvcc = enable_mvcc ? UseMvcc::Yes : UseMvcc::No;
  out << "- MVCC is " << (enable_mvcc ? "enabled" : "disabled") << std::endl;

  const auto enable_scheduler = json_config.value("scheduler", default_config.enable_scheduler);
  const auto cores = json_config.value("cores", default_config.cores);
  const auto number_of_cores_str = (cores == 0) ? "all available" : std::to_string(cores);
  const auto core_info = enable_scheduler ? " using " + number_of_cores_str + " cores" : "";
  out << "- Running in " + std::string(enable_scheduler ? "multi" : "single") + "-threaded mode" << core_info
      << std::endl;

  const auto clients = json_config.value("clients", default_config.clients);
  out << "- " + std::to_string(clients) + " simulated clients are scheduling queries in parallel" << std::endl;

  if (cores != default_config.cores || clients != default_config.clients) {
    if (!enable_scheduler) {
      PerformanceWarning("'--cores' or '--clients' specified but ignored, because '--scheduler' is false")
    }
  }

  // Determine benchmark and display it
  const auto benchmark_mode_str = json_config.value("mode", "IndividualQueries");
  auto benchmark_mode = BenchmarkMode::IndividualQueries;  // Just to init it deterministically
  if (benchmark_mode_str == "IndividualQueries") {
    benchmark_mode = BenchmarkMode::IndividualQueries;
  } else if (benchmark_mode_str == "PermutedQuerySet") {
    benchmark_mode = BenchmarkMode::PermutedQuerySet;
  } else {
    throw std::runtime_error("Invalid benchmark mode: '" + benchmark_mode_str + "'");
  }
  out << "- Running benchmark in '" << benchmark_mode_str << "' mode" << std::endl;

  const auto enable_visualization = json_config.value("visualize", default_config.enable_visualization);
  out << "- Visualization is " << (enable_visualization ? "on" : "off") << std::endl;

  // Get the specified encoding type
  std::unique_ptr<EncodingConfig> encoding_config{};
  const auto encoding_type_str = json_config.value("encoding", "Dictionary");
  const auto compression_type_str = json_config.value("compression", "");
  if (boost::algorithm::ends_with(encoding_type_str, ".json")) {
    // Use encoding file instead of default type
    encoding_config = std::make_unique<EncodingConfig>(parse_encoding_config(encoding_type_str));
    out << "- Encoding is custom from " << encoding_type_str << "" << std::endl;

    Assert(compression_type_str.empty(), "Specified both compression type and an encoding file. Invalid combination.");
  } else {
    encoding_config = std::make_unique<EncodingConfig>(
        EncodingConfig::encoding_spec_from_strings(encoding_type_str, compression_type_str));
    out << "- Encoding is '" << encoding_type_str << "'" << std::endl;
  }

  // Get all other variables
  const auto chunk_size = json_config.value("chunk_size", default_config.chunk_size);
  out << "- Chunk size is " << chunk_size << std::endl;

  const auto max_runs = json_config.value("runs", default_config.max_num_query_runs);
  out << "- Max runs per query is " << max_runs << std::endl;

  const auto default_duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(default_config.max_duration);
  const auto max_duration = json_config.value("time", default_duration_seconds.count());
  out << "- Max duration per query is " << max_duration << " seconds" << std::endl;
  const Duration timeout_duration = std::chrono::duration_cast<opossum::Duration>(std::chrono::seconds{max_duration});

  const auto default_warmup_seconds = std::chrono::duration_cast<std::chrono::seconds>(default_config.warmup_duration);
  const auto warmup = json_config.value("warmup", default_warmup_seconds.count());
  if (warmup > 0) {
    out << "- Warmup duration per query is " << warmup << " seconds" << std::endl;
  } else {
    out << "- No warmup runs are performed" << std::endl;
  }
  const Duration warmup_duration = std::chrono::duration_cast<opossum::Duration>(std::chrono::seconds{warmup});

  return BenchmarkConfig{benchmark_mode,       verbose,  chunk_size,       *encoding_config, max_runs, timeout_duration,
                         warmup_duration,      use_mvcc, output_file_path, enable_scheduler, cores,    clients,
                         enable_visualization, out};
}

BenchmarkConfig CLIConfigParser::parse_basic_cli_options(const cxxopts::ParseResult& parse_result) {
  return parse_basic_options_json_config(basic_cli_options_to_json(parse_result));
}

nlohmann::json CLIConfigParser::basic_cli_options_to_json(const cxxopts::ParseResult& parse_result) {
  nlohmann::json json_config;

  json_config.emplace("verbose", parse_result["verbose"].as<bool>());
  json_config.emplace("runs", parse_result["runs"].as<size_t>());
  json_config.emplace("chunk_size", parse_result["chunk_size"].as<ChunkOffset>());
  json_config.emplace("time", parse_result["time"].as<size_t>());
  json_config.emplace("warmup", parse_result["warmup"].as<size_t>());
  json_config.emplace("mode", parse_result["mode"].as<std::string>());
  json_config.emplace("encoding", parse_result["encoding"].as<std::string>());
  json_config.emplace("compression", parse_result["compression"].as<std::string>());
  json_config.emplace("scheduler", parse_result["scheduler"].as<bool>());
  json_config.emplace("cores", parse_result["cores"].as<uint>());
  json_config.emplace("clients", parse_result["clients"].as<uint>());
  json_config.emplace("mvcc", parse_result["mvcc"].as<bool>());
  json_config.emplace("visualize", parse_result["visualize"].as<bool>());
  json_config.emplace("output", parse_result["output"].as<std::string>());

  return json_config;
}

EncodingConfig CLIConfigParser::parse_encoding_config(const std::string& encoding_file_str) {
  Assert(filesystem::is_regular_file(encoding_file_str), "No such file: " + encoding_file_str);

  nlohmann::json encoding_config_json;
  std::ifstream json_file{encoding_file_str};
  json_file >> encoding_config_json;

  const auto encoding_spec_from_json = [](const nlohmann::json& json_spec) {
    Assert(json_spec.count("encoding"), "Need to specify encoding type.");
    const auto encoding_str = json_spec["encoding"];
    const auto compression_str = json_spec.value("compression", "");
    return EncodingConfig::encoding_spec_from_strings(encoding_str, compression_str);
  };

  Assert(encoding_config_json.count("default"), "Config must contain default encoding.");
  const auto default_spec = encoding_spec_from_json(encoding_config_json["default"]);

  DataTypeEncodingMapping type_encoding_mapping;
  const auto has_type_encoding = encoding_config_json.find("type") != encoding_config_json.end();
  if (has_type_encoding) {
    const auto type_encoding = encoding_config_json["type"];
    Assert(type_encoding.is_object(), "The type encoding needs to be specified as a json object.");

    for (const auto& type : nlohmann::json::iterator_wrapper(type_encoding)) {
      const auto type_str = boost::to_lower_copy(type.key());
      const auto data_type_it = data_type_to_string.right.find(type_str);
      Assert(data_type_it != data_type_to_string.right.end(), "Unknown data type for encoding: " + type_str);

      const auto& encoding_info = type.value();
      Assert(encoding_info.is_object(), "The type encoding info needs to be specified as a json object.");

      const auto data_type = data_type_it->second;
      type_encoding_mapping[data_type] = encoding_spec_from_json(encoding_info);
    }
  }

  TableSegmentEncodingMapping custom_encoding_mapping;
  const auto has_custom_encoding = encoding_config_json.find("custom") != encoding_config_json.end();

  if (has_custom_encoding) {
    const auto custom_encoding = encoding_config_json["custom"];
    Assert(custom_encoding.is_object(), "The custom table encoding needs to be specified as a json object.");

    for (const auto& table : nlohmann::json::iterator_wrapper(custom_encoding)) {
      const auto& table_name = table.key();
      const auto& columns = table.value();

      Assert(columns.is_object(), "The custom encoding for column types needs to be specified as a json object.");
      custom_encoding_mapping.emplace(table_name, std::unordered_map<std::string, SegmentEncodingSpec>());

      for (const auto& column : nlohmann::json::iterator_wrapper(columns)) {
        const auto& column_name = column.key();
        const auto& encoding_info = column.value();
        Assert(encoding_info.is_object(),
               "The custom encoding for column types needs to be specified as a json object.");
        custom_encoding_mapping[table_name][column_name] = encoding_spec_from_json(encoding_info);
      }
    }
  }

  return EncodingConfig{default_spec, std::move(type_encoding_mapping), std::move(custom_encoding_mapping)};
}

std::string CLIConfigParser::detailed_help(const cxxopts::Options& options) {
  return options.help() + BenchmarkConfig::description + EncodingConfig::description;
}

EncodingConfig::EncodingConfig() : EncodingConfig{SegmentEncodingSpec{EncodingType::Dictionary}} {}

EncodingConfig::EncodingConfig(const SegmentEncodingSpec& default_encoding_spec)
    : EncodingConfig{default_encoding_spec, {}, {}} {}

EncodingConfig::EncodingConfig(const SegmentEncodingSpec& default_encoding_spec,
                               DataTypeEncodingMapping type_encoding_mapping,
                               TableSegmentEncodingMapping encoding_mapping)
    : default_encoding_spec{default_encoding_spec},
      type_encoding_mapping{std::move(type_encoding_mapping)},
      custom_encoding_mapping{std::move(encoding_mapping)} {}

EncodingConfig EncodingConfig::unencoded() { return EncodingConfig{SegmentEncodingSpec{EncodingType::Unencoded}}; }

SegmentEncodingSpec EncodingConfig::encoding_spec_from_strings(const std::string& encoding_str,
                                                               const std::string& compression_str) {
  const auto encoding = EncodingConfig::encoding_string_to_type(encoding_str);
  const auto compression = EncodingConfig::compression_string_to_type(compression_str);

  return compression ? SegmentEncodingSpec{encoding, *compression} : SegmentEncodingSpec{encoding};
}

EncodingType EncodingConfig::encoding_string_to_type(const std::string& encoding_str) {
  const auto type = encoding_type_to_string.right.find(encoding_str);
  Assert(type != encoding_type_to_string.right.end(), "Invalid encoding type: '" + encoding_str + "'");
  return type->second;
}

std::optional<VectorCompressionType> EncodingConfig::compression_string_to_type(const std::string& compression_str) {
  if (compression_str.empty()) return std::nullopt;

  const auto compression = vector_compression_type_to_string.right.find(compression_str);
  Assert(compression != vector_compression_type_to_string.right.end(),
         "Invalid compression type: '" + compression_str + "'");
  return compression->second;
}

nlohmann::json EncodingConfig::to_json() const {
  const auto encoding_spec_to_string_map = [](const SegmentEncodingSpec& spec) {
    nlohmann::json mapping{};
    mapping["encoding"] = encoding_type_to_string.left.at(spec.encoding_type);
    if (spec.vector_compression_type) {
      mapping["compression"] = vector_compression_type_to_string.left.at(spec.vector_compression_type.value());
    }
    return mapping;
  };

  nlohmann::json json{};
  json["default"] = encoding_spec_to_string_map(default_encoding_spec);

  nlohmann::json type_mapping{};
  for (const auto& [type, spec] : type_encoding_mapping) {
    const auto& type_str = data_type_to_string.left.at(type);
    type_mapping[type_str] = encoding_spec_to_string_map(spec);
  }

  if (!type_mapping.empty()) {
    json["type"] = type_mapping;
  }

  nlohmann::json table_mapping{};
  for (const auto& [table, column_config] : custom_encoding_mapping) {
    nlohmann::json column_mapping{};
    for (const auto& [column, spec] : column_config) {
      column_mapping[column] = encoding_spec_to_string_map(spec);
    }
    table_mapping[table] = column_mapping;
  }

  if (!table_mapping.empty()) {
    json["custom"] = table_mapping;
  }

  return json;
}

void BenchmarkTableEncoder::encode(const std::string& table_name, const std::shared_ptr<Table>& table,
                                   const EncodingConfig& config) {
  const auto& type_mapping = config.type_encoding_mapping;
  const auto& custom_mapping = config.custom_encoding_mapping;

  const auto& column_mapping_it = custom_mapping.find(table_name);
  const auto table_has_custom_encoding = column_mapping_it != custom_mapping.end();

  ChunkEncodingSpec chunk_spec;

  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    if (table_has_custom_encoding) {
      const auto& column_name = table->column_name(column_id);
      const auto& encoding_by_column_name = column_mapping_it->second;
      const auto& segment_encoding = encoding_by_column_name.find(column_name);
      if (segment_encoding != encoding_by_column_name.end()) {
        // The column type has a custom encoding
        chunk_spec.push_back(segment_encoding->second);
        continue;
      }
    }

    const auto& column_type = table->column_data_type(column_id);
    const auto& encoding_by_data_type = type_mapping.find(column_type);
    if (encoding_by_data_type != type_mapping.end()) {
      // The column type has a specific encoding
      chunk_spec.push_back(encoding_by_data_type->second);
      continue;
    }

    // No custom or type encoding were specified, use default
    chunk_spec.push_back(config.default_encoding_spec);
  }

  return ChunkEncoder::encode_all_chunks(table, chunk_spec);
}

// This is intentionally limited to 80 chars per line, as cxxopts does this too and it looks bad otherwise.
const char* BenchmarkConfig::description = R"(
============================
Benchmark Configuration JSON
============================
All options can also be provided as a JSON config file. This must be the only
argument passed in. The options are identical to and behave like the CLI options.
Example:
{
  "verbose": true,
  "scheduler": true,
  "chunk_size": 10000,
  "time": 5
}

The JSON config can also include benchmark-specific options (e.g. TPCH's scale
option). They will be parsed like the
CLI options.

{
  "verbose": true,
  "scale": 0.1
}
)";

// This is intentionally limited to 80 chars per line, as cxxopts does this too and it looks bad otherwise.
const char* EncodingConfig::description = R"(
======================
Encoding Configuration
======================
The encoding config represents the segment encodings specified for a benchmark.
All segments of a given share column the same encoding.
If encoding (and vector compression) were specified via command line args,
all segments are compressed using the default encoding.
If a JSON config was provided, a column- and/or type-specific
encoding/compression can be chosen (same in each chunk). The JSON config must
look like this:

All encoding/compression types can be viewed with the `help` command or seen
in constant_mappings.cpp.
The encoding is always required, the compression is optional.

{
  "default": {
    "encoding": <ENCODING_TYPE_STRING>,               // required
    "compression": <VECTOR_COMPRESSION_TYPE_STRING>,  // optional
  },

  "type": {
    <DATA_TYPE>: {
      "encoding": <ENCODING_TYPE_STRING>,
      "compression": <VECTOR_COMPRESSION_TYPE_STRING>
    },
    <DATA_TYPE>: {
      "encoding": <ENCODING_TYPE_STRING>
    }
  },

  "custom": {
    <TABLE_NAME>: {
      <column_name>: {
        "encoding": <ENCODING_TYPE_STRING>,
        "compression": <VECTOR_COMPRESSION_TYPE_STRING>
      },
      <column_name>: {
        "encoding": <ENCODING_TYPE_STRING>
      }
    },
    <TABLE_NAME>: {
      <column_name>: {
        "encoding": <ENCODING_TYPE_STRING>,
        "compression": <VECTOR_COMPRESSION_TYPE_STRING>
      }
    }
  }
})";
}  // namespace opossum
