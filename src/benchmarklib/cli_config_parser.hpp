#pragma once

#include <filesystem>
#include <string>

#include "cxxopts.hpp"

#include "benchmark_config.hpp"
#include "encoding_config.hpp"

namespace opossum {

class CLIConfigParser {
 public:
  static bool cli_has_json_config(const int argc, char** argv);

  static nlohmann::json parse_json_config_file(const std::string& json_file_str);

  static nlohmann::json basic_cli_options_to_json(const cxxopts::ParseResult& parse_result);

  static BenchmarkConfig parse_basic_options_json_config(const nlohmann::json& json_config);

  static BenchmarkConfig parse_basic_cli_options(const cxxopts::ParseResult& parse_result);

  static EncodingConfig parse_encoding_config(const std::string& encoding_file_str);

  // Returns whether --help or --full_help was requested - used to stop execution of the benchmark
  static bool print_help_if_requested(const cxxopts::Options& options, const cxxopts::ParseResult& parse_result);
};

}  // namespace opossum
