#pragma once

#include <filesystem>
#include <string>

#include "cxxopts.hpp"

#include "benchmark_config.hpp"
#include "encoding_config.hpp"

namespace opossum {

class CLIConfigParser {
 public:
  static BenchmarkConfig parse_cli_options(const cxxopts::ParseResult& parse_result);

  static EncodingConfig parse_encoding_config(const std::string& encoding_file_str);

  // Returns whether --help or --full_help was requested - used to stop execution of the benchmark
  static bool print_help_if_requested(const cxxopts::Options& options, const cxxopts::ParseResult& parse_result);
};

}  // namespace opossum
