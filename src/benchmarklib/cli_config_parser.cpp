#include "cli_config_parser.hpp"

#include <fstream>
#include <iostream>

#include "boost/algorithm/string.hpp"

#include "constant_mappings.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

bool CLIConfigParser::cli_has_json_config(const int argc, char** argv) {
  const auto has_json = argc > 1 && boost::algorithm::ends_with(argv[1], ".json");
  if (has_json && argc > 2) {
    std::cout << "Passed multiple args with a json config. All CLI args will be ignored...";
  }

  return has_json;
}

nlohmann::json CLIConfigParser::parse_json_config_file(const std::string& json_file_str) {
  Assert(std::filesystem::is_regular_file(json_file_str), "No such file: " + json_file_str);

  nlohmann::json json_config;
  std::ifstream json_file{json_file_str};
  json_file >> json_config;

  return json_config;
}

BenchmarkConfig CLIConfigParser::parse_basic_options_json_config(const nlohmann::json& json_config) {
  const auto default_config = BenchmarkConfig::get_default_config();

  // Display info about output destination
  std::optional<std::string> output_file_path;
  const auto output_file_string = json_config.value("output", "");
  if (!output_file_string.empty()) {
    output_file_path = output_file_string;
    std::cout << "- Writing benchmark results to '" << *output_file_path << "'" << std::endl;
  }

  const auto enable_scheduler = json_config.value("scheduler", default_config.enable_scheduler);
  const auto cores = json_config.value("cores", default_config.cores);
  const auto number_of_cores_str = (cores == 0) ? "all available" : std::to_string(cores);
  const auto core_info = enable_scheduler ? " using " + number_of_cores_str + " cores" : "";
  std::cout << "- Running in " + std::string(enable_scheduler ? "multi" : "single") + "-threaded mode" << core_info
            << std::endl;

  const auto clients = json_config.value("clients", default_config.clients);
  std::cout << "- " + std::to_string(clients) + " simulated clients are scheduling items in parallel" << std::endl;

  if (cores != default_config.cores || clients != default_config.clients) {
    if (!enable_scheduler) {
      PerformanceWarning("'--cores' or '--clients' specified but ignored, because '--scheduler' is false");
    }
  }

  Assert(clients > 0, "Invalid value for --clients");

  if (enable_scheduler && clients == 1) {
    std::cout << "\n\n- WARNING: You are running in multi-threaded (MT) mode but have set --clients=1.\n";
    std::cout << "           You will achieve better MT performance by executing multiple queries in parallel.\n";
    std::cout << std::endl;
  }

  // Determine benchmark and display it
  const auto benchmark_mode_str = json_config.value("mode", "Ordered");
  auto benchmark_mode = BenchmarkMode::Ordered;  // Just to init it deterministically
  if (benchmark_mode_str == "Ordered") {
    benchmark_mode = BenchmarkMode::Ordered;
  } else if (benchmark_mode_str == "Shuffled") {
    benchmark_mode = BenchmarkMode::Shuffled;
  } else {
    throw std::runtime_error("Invalid benchmark mode: '" + benchmark_mode_str + "'");
  }
  std::cout << "- Running benchmark in '" << benchmark_mode_str << "' mode" << std::endl;

  const auto enable_visualization = json_config.value("visualize", default_config.enable_visualization);
  if (enable_visualization) {
    std::cout << "- Visualizing the plans into SVG files. This will make the performance numbers invalid." << std::endl;
  }

  // Get the specified encoding type
  std::unique_ptr<EncodingConfig> encoding_config{};
  const auto encoding_type_str = json_config.value("encoding", "Dictionary");
  const auto compression_type_str = json_config.value("compression", "");
  if (boost::algorithm::ends_with(encoding_type_str, ".json")) {
    // Use encoding file instead of default type
    encoding_config = std::make_unique<EncodingConfig>(parse_encoding_config(encoding_type_str));
    std::cout << "- Encoding is custom from " << encoding_type_str << "" << std::endl;

    Assert(compression_type_str.empty(), "Specified both compression type and an encoding file. Invalid combination.");
  } else {
    encoding_config = std::make_unique<EncodingConfig>(
        EncodingConfig::encoding_spec_from_strings(encoding_type_str, compression_type_str));
    std::cout << "- Encoding is '" << encoding_type_str << "'" << std::endl;
  }

  const auto indexes = json_config.value("indexes", default_config.indexes);
  if (indexes) {
    std::cout << "- Creating indexes (as defined by the benchmark)" << std::endl;
  }

  // Get all other variables
  const auto chunk_size = json_config.value("chunk_size", default_config.chunk_size);
  std::cout << "- Chunk size is " << chunk_size << std::endl;

  const auto max_runs = json_config.value("runs", default_config.max_runs);
  std::cout << "- Max runs per item is " << max_runs << std::endl;

  const auto default_duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(default_config.max_duration);
  const auto max_duration = json_config.value("time", default_duration_seconds.count());
  std::cout << "- Max duration per item is " << max_duration << " seconds" << std::endl;
  const Duration timeout_duration = std::chrono::duration_cast<opossum::Duration>(std::chrono::seconds{max_duration});

  const auto default_warmup_seconds = std::chrono::duration_cast<std::chrono::seconds>(default_config.warmup_duration);
  const auto warmup = json_config.value("warmup", default_warmup_seconds.count());
  if (warmup > 0) {
    std::cout << "- Warmup duration per item is " << warmup << " seconds" << std::endl;
  } else {
    std::cout << "- No warmup runs are performed" << std::endl;
  }
  const Duration warmup_duration = std::chrono::duration_cast<opossum::Duration>(std::chrono::seconds{warmup});

  const auto verify = json_config.value("verify", default_config.verify);
  if (verify) {
    std::cout << "- Automatically verifying results with SQLite. This will make the performance numbers invalid."
              << std::endl;
  }

  const auto cache_binary_tables = json_config.value("cache_binary_tables", false);
  if (cache_binary_tables) {
    std::cout << "- Caching tables as binary files" << std::endl;
  } else {
    std::cout << "- Not caching tables as binary files" << std::endl;
  }

  const auto sql_metrics = json_config.value("sql_metrics", false);
  if (sql_metrics) {
    std::cout << "- Tracking SQL metrics" << std::endl;
  } else {
    std::cout << "- Not tracking SQL metrics" << std::endl;
  }

  return BenchmarkConfig{
      benchmark_mode,  chunk_size,          *encoding_config, indexes, max_runs, timeout_duration,
      warmup_duration, output_file_path,    enable_scheduler, cores,   clients,  enable_visualization,
      verify,          cache_binary_tables, sql_metrics};
}

BenchmarkConfig CLIConfigParser::parse_basic_cli_options(const cxxopts::ParseResult& parse_result) {
  return parse_basic_options_json_config(basic_cli_options_to_json(parse_result));
}

nlohmann::json CLIConfigParser::basic_cli_options_to_json(const cxxopts::ParseResult& parse_result) {
  nlohmann::json json_config;

  json_config.emplace("runs", parse_result["runs"].as<size_t>());
  json_config.emplace("chunk_size", parse_result["chunk_size"].as<ChunkOffset>());
  json_config.emplace("time", parse_result["time"].as<size_t>());
  json_config.emplace("warmup", parse_result["warmup"].as<size_t>());
  json_config.emplace("mode", parse_result["mode"].as<std::string>());
  json_config.emplace("encoding", parse_result["encoding"].as<std::string>());
  json_config.emplace("compression", parse_result["compression"].as<std::string>());
  json_config.emplace("indexes", parse_result["indexes"].as<bool>());
  json_config.emplace("scheduler", parse_result["scheduler"].as<bool>());
  json_config.emplace("cores", parse_result["cores"].as<uint>());
  json_config.emplace("clients", parse_result["clients"].as<uint>());
  json_config.emplace("visualize", parse_result["visualize"].as<bool>());
  json_config.emplace("output", parse_result["output"].as<std::string>());
  json_config.emplace("verify", parse_result["verify"].as<bool>());
  json_config.emplace("cache_binary_tables", parse_result["cache_binary_tables"].as<bool>());
  json_config.emplace("sql_metrics", parse_result["sql_metrics"].as<bool>());

  return json_config;
}

EncodingConfig CLIConfigParser::parse_encoding_config(const std::string& encoding_file_str) {
  Assert(std::filesystem::is_regular_file(encoding_file_str), "No such file: " + encoding_file_str);

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

bool CLIConfigParser::print_help_if_requested(const cxxopts::Options& options,
                                              const cxxopts::ParseResult& parse_result) {
  if (!parse_result.count("help") && !parse_result.count("full_help")) {
    return false;
  }

  std::cout << options.help() << std::endl;

  if (parse_result.count("full_help")) {
    std::cout << BenchmarkConfig::description << std::endl;
    std::cout << EncodingConfig::description << std::endl;
  } else {
    std::cout << "Use --full_help for more configuration options" << std::endl << std::endl;
  }

  return true;
}

}  // namespace opossum
