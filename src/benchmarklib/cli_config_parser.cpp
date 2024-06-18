#include "cli_config_parser.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>

#include "cxxopts.hpp"
#include "nlohmann/json.hpp"

#include "all_type_variant.hpp"
#include "benchmark_config.hpp"
#include "encoding_config.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace hyrise {

std::shared_ptr<BenchmarkConfig> CLIConfigParser::parse_cli_options(const cxxopts::ParseResult& parse_result) {
  const auto default_config = BenchmarkConfig{};

  // Display info about output destination.
  auto output_file_path = std::optional<std::string>{};
  const auto output_file_string = parse_result["output"].as<std::string>();
  if (!output_file_string.empty()) {
    output_file_path = output_file_string;
    std::cout << "- Writing benchmark results to '" << *output_file_path << "'\n";
  }

  const auto enable_scheduler = parse_result["scheduler"].as<bool>();
  const auto cores = parse_result["cores"].as<uint32_t>();
  const auto number_of_cores_str = (cores == 0) ? "all available" : std::to_string(cores);
  const auto core_info = enable_scheduler ? " using " + number_of_cores_str + " cores" : "";
  std::cout << "- Running in " + std::string(enable_scheduler ? "multi" : "single") + "-threaded mode" << core_info
            << '\n';
  const auto data_preparation_cores = parse_result["data_preparation_cores"].as<uint32_t>();
  const auto number_of_data_preparation_cores_str =
      (data_preparation_cores == 0) ? "all available" : std::to_string(data_preparation_cores);
  std::cout << "- Data preparation will use " << number_of_data_preparation_cores_str
            << (data_preparation_cores == 1 ? " core" : " cores") << '\n';

  const auto clients = parse_result["clients"].as<uint32_t>();
  std::cout << "- " + std::to_string(clients) + " simulated ";
  std::cout << (clients == 1 ? "client is " : "clients are ") << "scheduling items";
  std::cout << (clients > 1 ? " in parallel" : "") << '\n';

  if (cores != default_config.cores || clients != default_config.clients) {
    if (!enable_scheduler) {
      PerformanceWarning("'--cores' or '--clients' specified but ignored, because '--scheduler' is false");
    }
  }

  Assert(clients > 0, "Invalid value for --clients.");

  if (enable_scheduler && clients == 1) {
    std::cout << "\n\n- WARNING: You are running in multi-threaded (MT) mode but have set --clients=1.\n";
    std::cout << "           You will achieve better MT performance by executing multiple queries in parallel\n\n";
  }

  // Determine benchmark and display it
  const auto benchmark_mode_str = parse_result["mode"].as<std::string>();
  auto benchmark_mode = BenchmarkMode::Ordered;  // Just to init it deterministically.
  if (benchmark_mode_str == "Ordered") {
    benchmark_mode = BenchmarkMode::Ordered;
  } else if (benchmark_mode_str == "Shuffled") {
    benchmark_mode = BenchmarkMode::Shuffled;
  } else {
    throw std::runtime_error("Invalid benchmark mode: '" + benchmark_mode_str + "'");
  }
  std::cout << "- Running benchmark in '" << benchmark_mode_str << "' mode\n";

  const auto enable_visualization = parse_result["visualize"].as<bool>();
  if (enable_visualization) {
    Assert(clients == 1, "Cannot visualize plans with multiple clients as files may be overwritten.");
    std::cout << "- Visualizing the plans into SVG files. This will make the performance numbers invalid\n";
  }

  // Get the specified encoding type.
  auto encoding_config = std::unique_ptr<EncodingConfig>{};
  const auto encoding_type_str = parse_result["encoding"].as<std::string>();
  const auto compression_type_str = parse_result["compression"].as<std::string>();
  if (boost::algorithm::ends_with(encoding_type_str, ".json")) {
    // Use encoding file instead of default type.
    encoding_config = std::make_unique<EncodingConfig>(parse_encoding_config(encoding_type_str));
    std::cout << "- Encoding is custom from " << encoding_type_str << "\n";

    Assert(compression_type_str.empty(), "Specified both compression type and an encoding file. Invalid combination.");
  } else {
    encoding_config = std::make_unique<EncodingConfig>(
        EncodingConfig::encoding_spec_from_strings(encoding_type_str, compression_type_str));
    std::cout << "- Encoding is '" << encoding_type_str << "'\n";
  }

  const auto chunk_indexes = parse_result["chunk_indexes"].as<bool>();
  if (chunk_indexes) {
    std::cout << "- Creating chunk indexes (separate index per chunk; columns defined by benchmark)\n";
  }

  const auto table_indexes = parse_result["table_indexes"].as<bool>();
  if (table_indexes) {
    std::cout << "- Creating table indexes (index per table column; columns defined by benchmark)\n";
  }

  if (chunk_indexes && table_indexes) {
    std::cout << "WARNING: Creating chunk and table indexes simultaneously.\n";
  }

  // Get all other variables.
  const auto chunk_size = parse_result["chunk_size"].as<ChunkOffset>();
  std::cout << "- Chunk size is " << chunk_size << '\n';

  const auto max_runs = parse_result["runs"].as<int64_t>();
  if (max_runs >= 0) {
    std::cout << "- Max runs per item is " << max_runs << '\n';
  } else {
    std::cout << "- Executing items until max duration is up:\n";
  }

  const auto max_duration = parse_result["time"].as<uint64_t>();
  std::cout << "- Max duration per item is " << max_duration << " seconds\n";
  const Duration timeout_duration = std::chrono::seconds{max_duration};

  const auto warmup = parse_result["warmup"].as<uint64_t>();
  if (warmup > 0) {
    std::cout << "- Warmup duration per item is " << warmup << " seconds\n";
  } else {
    std::cout << "- No warmup runs are performed\n";
  }
  const Duration warmup_duration = std::chrono::seconds{warmup};

  const auto verify = parse_result["verify"].as<bool>();
  if (verify) {
    std::cout << "- Automatically verifying results with SQLite. This will make the performance numbers invalid\n";
  }

  const auto cache_binary_tables = !parse_result["dont_cache_binary_tables"].as<bool>();
  if (cache_binary_tables) {
    std::cout << "- Caching tables as binary files\n";
  } else {
    std::cout << "- Not caching tables as binary files\n";
  }

  const auto system_metrics = parse_result["system_metrics"].as<bool>();
  if (system_metrics) {
    Assert(!output_file_string.empty(), "--system_metrics only makes sense when an output file is set.");
    std::cout << "- Tracking system metrics\n";
  } else {
    std::cout << "- Not tracking system metrics\n";
  }

  const auto pipeline_metrics = parse_result["pipeline_metrics"].as<bool>();
  if (pipeline_metrics) {
    Assert(!output_file_string.empty(), "--pipeline_metrics only makes sense when an output file is set.");
    std::cout << "- Tracking SQL pipeline metrics\n";
  } else {
    std::cout << "- Not tracking SQL pipeline metrics\n";
  }

  auto plugins = std::vector<std::string>{};
  auto comma_separated_plugins = parse_result["plugins"].as<std::string>();
  if (!comma_separated_plugins.empty()) {
    boost::trim_if(comma_separated_plugins, boost::is_any_of(","));
    boost::split(plugins, comma_separated_plugins, boost::is_any_of(","), boost::token_compress_on);
  }

  return std::make_shared<BenchmarkConfig>(
      benchmark_mode, chunk_size, *encoding_config, chunk_indexes, table_indexes, max_runs, timeout_duration,
      warmup_duration, output_file_path, enable_scheduler, cores, data_preparation_cores, clients, enable_visualization,
      verify, cache_binary_tables, system_metrics, pipeline_metrics, plugins);
}

EncodingConfig CLIConfigParser::parse_encoding_config(const std::string& encoding_file_str) {
  Assert(std::filesystem::is_regular_file(encoding_file_str), "No such file: " + encoding_file_str);

  auto encoding_config_json = nlohmann::json{};
  auto json_file = std::ifstream{encoding_file_str};
  json_file >> encoding_config_json;

  const auto encoding_spec_from_json = [](const nlohmann::json& json_spec) {
    Assert(json_spec.count("encoding"), "Need to specify encoding type.");
    const auto& encoding_str = json_spec["encoding"];
    const auto compression_str = json_spec.value("compression", "");
    return EncodingConfig::encoding_spec_from_strings(encoding_str, compression_str);
  };

  Assert(encoding_config_json.count("default"), "Config must contain default encoding.");
  const auto default_spec = encoding_spec_from_json(encoding_config_json["default"]);

  auto type_encoding_mapping = DataTypeEncodingMapping{};
  const auto has_type_encoding = encoding_config_json.find("type") != encoding_config_json.end();
  if (has_type_encoding) {
    const auto type_encoding = encoding_config_json["type"];
    Assert(type_encoding.is_object(), "The type encoding needs to be specified as a json object.");

    for (const auto& type : type_encoding.items()) {
      const auto type_str = boost::to_lower_copy(type.key());
      const auto data_type_it = data_type_to_string.right.find(type_str);
      Assert(data_type_it != data_type_to_string.right.end(), "Unknown data type for encoding: " + type_str);

      const auto& encoding_info = type.value();
      Assert(encoding_info.is_object(), "The type encoding info needs to be specified as a json object.");

      const auto data_type = data_type_it->second;
      type_encoding_mapping[data_type] = encoding_spec_from_json(encoding_info);
    }
  }

  auto custom_encoding_mapping = TableSegmentEncodingMapping{};
  const auto has_custom_encoding = encoding_config_json.find("custom") != encoding_config_json.end();

  if (has_custom_encoding) {
    const auto custom_encoding = encoding_config_json["custom"];
    Assert(custom_encoding.is_object(), "The custom table encoding needs to be specified as a json object.");

    for (const auto& table : custom_encoding.items()) {
      const auto& table_name = table.key();
      const auto& columns = table.value();

      Assert(columns.is_object(), "The custom encoding for column types needs to be specified as a json object.");
      custom_encoding_mapping.emplace(table_name, std::unordered_map<std::string, SegmentEncodingSpec>());

      for (const auto& column : columns.items()) {
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

bool CLIConfigParser::print_help_if_requested(const cxxopts::Options& options,
                                              const cxxopts::ParseResult& parse_result) {
  if (parse_result.count("help") == 0 && parse_result.count("full_help") == 0) {
    return false;
  }

  std::cout << options.help() << '\n';

  if (parse_result.count("full_help") > 0) {
    std::cout << EncodingConfig::description << '\n';
  } else {
    std::cout << "Use --full_help for more configuration options\n\n";
  }

  return true;
}

}  // namespace hyrise
